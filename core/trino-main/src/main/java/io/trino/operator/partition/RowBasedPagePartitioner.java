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
package io.trino.operator.partition;

import io.airlift.units.DataSize;
import io.trino.execution.buffer.OutputBuffer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.operator.OperatorContext;
import io.trino.operator.PartitionFunction;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public class RowBasedPagePartitioner
        extends BuilderPagePartitioner
{
    public RowBasedPagePartitioner(
            PartitionFunction partitionFunction,
            List<Integer> partitionChannels,
            List<Optional<NullableValue>> partitionConstants,
            boolean replicatesAnyRow,
            OptionalInt nullChannel,
            OutputBuffer outputBuffer,
            PagesSerdeFactory serdeFactory,
            List<Type> sourceTypes,
            DataSize maxMemory,
            OperatorContext operatorContext)
    {
        super(partitionFunction, partitionChannels, partitionConstants, replicatesAnyRow, nullChannel, outputBuffer, serdeFactory, sourceTypes, maxMemory, operatorContext);
    }

    @Override
    public void partitionPage(Page page)
    {
        requireNonNull(page, "page is null");
        if (page.getPositionCount() == 0) {
            return;
        }

        int position;
        // Handle "any row" replication outside of the inner loop processing
        if (replicatesAnyRow && !hasAnyRowBeenReplicated) {
            for (PageBuilder pageBuilder : pageBuilders) {
                appendRow(pageBuilder, page, 0);
            }
            hasAnyRowBeenReplicated = true;
            position = 1;
        }
        else {
            position = 0;
        }

        Page partitionFunctionArgs = getPartitionFunctionArguments(page);
        // Skip null block checks if mayHaveNull reports that no positions will be null
        if (nullChannel >= 0 && page.getBlock(nullChannel).mayHaveNull()) {
            Block nullsBlock = page.getBlock(nullChannel);
            for (; position < page.getPositionCount(); position++) {
                if (nullsBlock.isNull(position)) {
                    for (PageBuilder pageBuilder : pageBuilders) {
                        appendRow(pageBuilder, page, position);
                    }
                }
                else {
                    int partition = partitionFunction.getPartition(partitionFunctionArgs, position);
                    appendRow(pageBuilders[partition], page, position);
                }
            }
        }
        else {
            for (; position < page.getPositionCount(); position++) {
                int partition = partitionFunction.getPartition(partitionFunctionArgs, position);
                appendRow(pageBuilders[partition], page, position);
            }
        }

        flush(false);
    }

    private void appendRow(PageBuilder pageBuilder, Page page, int position)
    {
        pageBuilder.declarePosition();

        for (int channel = 0; channel < sourceTypes.length; channel++) {
            Type type = sourceTypes[channel];
            type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
        }
    }
}
