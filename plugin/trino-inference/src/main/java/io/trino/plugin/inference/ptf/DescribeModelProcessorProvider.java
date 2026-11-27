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
package io.trino.plugin.inference.ptf;

import io.trino.plugin.inference.ptf.DescribeModelTableFunction.DescribeModelFunctionHandle;
import io.trino.plugin.inference.ptf.DescribeModelTableFunction.ModelTensor;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.inference.ptf.DescribeModelTableFunction.COLUMN_TYPES;
import static io.trino.spi.function.table.TableFunctionProcessorState.Finished.FINISHED;
import static io.trino.spi.function.table.TableFunctionProcessorState.Processed.produced;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class DescribeModelProcessorProvider
        implements TableFunctionProcessorProvider
{
    @Override
    public TableFunctionSplitProcessor getSplitProcessor(
            ConnectorSession session,
            ConnectorTableFunctionHandle handle,
            Optional<ConnectorTableCredentials> tableCredentials,
            ConnectorSplit split)
    {
        return new DescribeModelSplitProcessor(((DescribeModelFunctionHandle) handle).tensors());
    }

    static class DescribeModelSplitProcessor
            implements TableFunctionSplitProcessor
    {
        private final List<ModelTensor> tensors;
        private boolean finished;

        public DescribeModelSplitProcessor(List<ModelTensor> tensors)
        {
            this.tensors = tensors;
        }

        @Override
        public TableFunctionProcessorState process()
        {
            if (finished) {
                return FINISHED;
            }
            finished = true;

            PageBuilder pageBuilder = new PageBuilder(COLUMN_TYPES);
            for (ModelTensor tensor : tensors) {
                pageBuilder.declarePosition();
                VARCHAR.writeString(pageBuilder.getBlockBuilder(0), tensor.kind());
                VARCHAR.writeString(pageBuilder.getBlockBuilder(1), tensor.name());
                VARCHAR.writeString(pageBuilder.getBlockBuilder(2), tensor.dataType());
                ((ArrayBlockBuilder) pageBuilder.getBlockBuilder(3)).buildEntry(elementBuilder -> tensor.shape().forEach(dimension -> BIGINT.writeLong(elementBuilder, dimension)));
                BlockBuilder trinoType = pageBuilder.getBlockBuilder(4);
                tensor.trinoType().ifPresentOrElse(value -> VARCHAR.writeString(trinoType, value), trinoType::appendNull);
            }
            return produced(pageBuilder.build());
        }
    }
}
