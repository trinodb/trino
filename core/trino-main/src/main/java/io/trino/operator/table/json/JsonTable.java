/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.table.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.operator.table.json.execution.JsonTableProcessingFragment;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.operator.scalar.json.ParameterUtil.getParametersArray;
import static io.trino.operator.table.json.execution.ExecutionPlanner.getExecutionPlan;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.usedInput;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.type.Json2016Type.JSON_2016;
import static java.util.Objects.requireNonNull;

/**
 * Implements feature ISO/IEC 9075-2:2023(E) 7.11 'JSON table'
 * including features T824, T827, T838
 */
public class JsonTable
{
    private JsonTable() {}

    /**
     * This class comprises all information necessary to execute the json_table function:
     *
     * @param processingPlan the root of the processing plan tree
     * @param outer the parent-child relationship between the input relation and the processingPlan result
     * @param errorOnError the error behavior: true for ERROR ON ERROR, false for EMPTY ON ERROR
     * @param parametersType type of the row containing JSON path parameters for the root JSON path. The function expects the parameters row in the channel 1.
     * Other channels in the input page correspond to JSON context item (channel 0), and default values for the value columns. Each value column in the processingPlan
     * knows the indexes of its default channels.
     * @param outputTypes types of the proper columns produced by the function
     */
    public record JsonTableFunctionHandle(JsonTablePlanNode processingPlan, boolean outer, boolean errorOnError, Type parametersType, Type[] outputTypes)
            implements ConnectorTableFunctionHandle
    {
        public JsonTableFunctionHandle
        {
            requireNonNull(processingPlan, "processingPlan is null");
            requireNonNull(parametersType, "parametersType is null");
            requireNonNull(outputTypes, "outputTypes is null");

            // We can't use RowType in the public interface because it's not directly deserializeable from JSON. See TypeDeserializerModule.
            checkArgument(parametersType instanceof RowType, "parametersType is not a row type");
        }
    }

    public static TableFunctionProcessorProvider getJsonTableFunctionProcessorProvider(Metadata metadata, TypeManager typeManager, FunctionManager functionManager)
    {
        return new TableFunctionProcessorProvider()
        {
            @Override
            public TableFunctionDataProcessor getDataProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle)
            {
                JsonTableFunctionHandle jsonTableFunctionHandle = (JsonTableFunctionHandle) handle;
                Object[] newRow = new Object[jsonTableFunctionHandle.outputTypes().length];
                JsonTableProcessingFragment executionPlan = getExecutionPlan(
                        jsonTableFunctionHandle.processingPlan(),
                        newRow,
                        jsonTableFunctionHandle.errorOnError(),
                        jsonTableFunctionHandle.outputTypes(),
                        session,
                        metadata,
                        typeManager,
                        functionManager);
                return new JsonTableFunctionProcessor(executionPlan, newRow, jsonTableFunctionHandle.outputTypes(), (RowType) jsonTableFunctionHandle.parametersType(), jsonTableFunctionHandle.outer());
            }
        };
    }

    public static class JsonTableFunctionProcessor
            implements TableFunctionDataProcessor
    {
        private final PageBuilder pageBuilder;
        private final int properColumnsCount;
        private final JsonTableProcessingFragment executionPlan;
        private final Object[] newRow;
        private final RowType parametersType;
        private final boolean outer;

        private long totalPositionsProcessed;
        private int currentPosition = -1;
        private boolean currentPositionAlreadyProduced;

        public JsonTableFunctionProcessor(JsonTableProcessingFragment executionPlan, Object[] newRow, Type[] outputTypes, RowType parametersType, boolean outer)
        {
            this.pageBuilder = new PageBuilder(ImmutableList.<Type>builder()
                    .add(outputTypes)
                    .add(BIGINT) // add additional position for pass-through index
                    .build());
            this.properColumnsCount = outputTypes.length;
            this.executionPlan = requireNonNull(executionPlan, "executionPlan is null");
            this.newRow = requireNonNull(newRow, "newRow is null");
            this.parametersType = requireNonNull(parametersType, "parametersType is null");
            this.outer = outer;
        }

        @Override
        public TableFunctionProcessorState process(List<Optional<Page>> input)
        {
            // no more input pages
            if (input == null) {
                if (pageBuilder.isEmpty()) {
                    return FINISHED;
                }
                return flushPageBuilder();
            }

            Page inputPage = getOnlyElement(input).orElseThrow();
            while (!pageBuilder.isFull()) {
                // new input page
                if (currentPosition == -1) {
                    if (inputPage.getPositionCount() == 0) {
                        return usedInput();
                    }
                    else {
                        currentPosition = 0;
                        currentPositionAlreadyProduced = false;
                        totalPositionsProcessed++;
                        SqlRow parametersRow = (SqlRow) readNativeValue(parametersType, inputPage.getBlock(1), currentPosition);
                        executionPlan.resetRoot(
                                (JsonNode) readNativeValue(JSON_2016, inputPage.getBlock(0), currentPosition),
                                inputPage,
                                currentPosition,
                                getParametersArray(parametersType, parametersRow));
                    }
                }

                // try to get output row for the current position (one position can produce multiple rows)
                boolean gotNewRow = executionPlan.getRow();
                if (gotNewRow) {
                    currentPositionAlreadyProduced = true;
                    addOutputRow();
                }
                else {
                    if (outer && !currentPositionAlreadyProduced) {
                        addNullPaddedRow();
                    }
                    // go to next position in the input page
                    currentPosition++;
                    if (currentPosition < inputPage.getPositionCount()) {
                        currentPositionAlreadyProduced = false;
                        totalPositionsProcessed++;
                        SqlRow parametersRow = (SqlRow) readNativeValue(parametersType, inputPage.getBlock(1), currentPosition);
                        executionPlan.resetRoot(
                                (JsonNode) readNativeValue(JSON_2016, inputPage.getBlock(0), currentPosition),
                                inputPage,
                                currentPosition,
                                getParametersArray(parametersType, parametersRow));
                    }
                    else {
                        currentPosition = -1;
                        return usedInput();
                    }
                }
            }

            return flushPageBuilder();
        }

        private TableFunctionProcessorState flushPageBuilder()
        {
            TableFunctionProcessorState result = produced(pageBuilder.build());
            pageBuilder.reset();
            return result;
        }

        private void addOutputRow()
        {
            pageBuilder.declarePosition();
            for (int channel = 0; channel < properColumnsCount; channel++) {
                writeNativeValue(pageBuilder.getType(channel), pageBuilder.getBlockBuilder(channel), newRow[channel]);
            }
            // pass-through index from partition start
            BIGINT.writeLong(pageBuilder.getBlockBuilder(properColumnsCount), totalPositionsProcessed - 1);
        }

        private void addNullPaddedRow()
        {
            Arrays.fill(newRow, null);
            addOutputRow();
        }
    }
}
