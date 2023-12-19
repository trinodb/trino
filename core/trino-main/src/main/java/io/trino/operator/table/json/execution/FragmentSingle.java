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
package io.trino.operator.table.json.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.trino.json.JsonPathEvaluator;
import io.trino.json.ir.IrJsonPath;
import io.trino.metadata.FunctionManager;
import io.trino.metadata.Metadata;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static io.trino.operator.table.json.execution.SequenceEvaluator.getSequence;
import static java.util.Objects.requireNonNull;

public class FragmentSingle
        implements JsonTableProcessingFragment
{
    private static final Object[] NO_PARAMETERS = new Object[] {};

    private final JsonPathEvaluator pathEvaluator;
    private final List<Column> columns;
    private final boolean errorOnError;
    private final boolean outer;
    private final JsonTableProcessingFragment child;
    private final int[] outputLayout;

    // the place where the computed values (or nulls) are stored while computing an output row
    private final Object[] newRow;

    private Page input;
    private int position;
    private List<JsonNode> sequence;
    private int nextItemIndex;

    // start processing next item from the sequence
    private boolean processNextItem;

    // indicates if we need to produce null-padded row for OUTER
    private boolean childAlreadyProduced;

    public FragmentSingle(
            IrJsonPath path,
            List<Column> columns,
            boolean errorOnError,
            boolean outer,
            JsonTableProcessingFragment child,
            Object[] newRow,
            ConnectorSession session,
            Metadata metadata,
            TypeManager typeManager,
            FunctionManager functionManager)
    {
        requireNonNull(path, "path is null");
        this.pathEvaluator = new JsonPathEvaluator(path, session, metadata, typeManager, functionManager);
        this.columns = ImmutableList.copyOf(columns);
        this.errorOnError = errorOnError;
        this.outer = outer;
        this.child = requireNonNull(child, "child is null");
        this.outputLayout = IntStream.concat(
                        columns.stream()
                                .mapToInt(Column::getOutputIndex),
                        Arrays.stream(child.getOutputLayout()))
                .toArray();
        this.newRow = requireNonNull(newRow, "newRow is null");
    }

    @Override
    public void reset(JsonNode item, Page input, int position)
    {
        resetRoot(item, input, position, NO_PARAMETERS);
    }

    /**
     * FragmentSingle can be the root Fragment. The root fragment is the only fragment that may have path parameters.
     * Prepares the root Fragment to produce rows for the new JSON item and a set of path parameters.
     */
    @Override
    public void resetRoot(JsonNode item, Page input, int position, Object[] pathParameters)
    {
        requireNonNull(pathParameters, "pathParameters is null");
        this.input = requireNonNull(input, "input is null");
        this.position = position;
        this.nextItemIndex = 0;
        this.processNextItem = true;
        this.sequence = getSequence(item, pathParameters, pathEvaluator, errorOnError);
    }

    /**
     * All values produced by the columns are stored on corresponding positions in `newRow`.
     * The values in `newRow` are not cleared between  subsequent calls to `getRow()`, so the values for columns are automatically reused during iterating over child.
     */
    @Override
    public boolean getRow()
    {
        while (true) {
            if (processNextItem) {
                if (nextItemIndex >= sequence.size()) {
                    // fragment is finished
                    return false;
                }
                JsonNode currentItem = sequence.get(nextItemIndex);
                nextItemIndex++; // it is correct to pass the updated value to `column.evaluate()` because ordinality numbers are 1-based according to ISO/IEC 9075-2:2016(E) 7.11 <JSON table> p.461 General rules.
                for (Column column : columns) {
                    newRow[column.getOutputIndex()] = column.evaluate(nextItemIndex, currentItem, input, position);
                }
                child.reset(currentItem, input, position);
                childAlreadyProduced = false;
                processNextItem = false;
            }

            boolean childProducedRow = child.getRow();
            if (childProducedRow) {
                childAlreadyProduced = true;
                return true;
            }

            // child is finished
            processNextItem = true;
            if (outer && !childAlreadyProduced) {
                appendNulls(child);
                return true;
            }
        }
    }

    private void appendNulls(JsonTableProcessingFragment fragment)
    {
        for (int column : fragment.getOutputLayout()) {
            newRow[column] = null;
        }
    }

    @Override
    public int[] getOutputLayout()
    {
        return outputLayout;
    }
}
