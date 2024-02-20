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
package io.trino.operator;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.block.RowBlock.getRowFieldsFromBlock;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Objects.requireNonNull;

/**
 * The transformPage() method in this class does two things:
 * <ul>
 *     <li>Transform the input page into an "update" page format</li>
 *     <li>Removes all rows whose operation number is DEFAULT_CASE_OPERATION_NUMBER</li>
 * </ul>
 */
public class ChangeOnlyUpdatedColumnsMergeProcessor
        implements MergeRowChangeProcessor
{
    private static final Block INSERT_FROM_UPDATE_BLOCK = nativeValueToBlock(TINYINT, 0L);

    private final int rowIdChannel;
    private final int mergeRowChannel;
    private final List<Integer> dataColumnChannels;
    private final int writeRedistributionColumnCount;

    public ChangeOnlyUpdatedColumnsMergeProcessor(
            int rowIdChannel,
            int mergeRowChannel,
            List<Integer> dataColumnChannels,
            List<Integer> redistributionColumnChannels)
    {
        this.rowIdChannel = rowIdChannel;
        this.mergeRowChannel = mergeRowChannel;
        this.dataColumnChannels = requireNonNull(dataColumnChannels, "dataColumnChannels is null");
        this.writeRedistributionColumnCount = redistributionColumnChannels.size();
    }

    @Override
    public Page transformPage(Page inputPage)
    {
        requireNonNull(inputPage, "inputPage is null");

        int inputChannelCount = inputPage.getChannelCount();
        checkArgument(inputChannelCount >= 2 + writeRedistributionColumnCount, "inputPage channelCount (%s) should be >= 2 + %s", inputChannelCount, writeRedistributionColumnCount);
        int positionCount = inputPage.getPositionCount();
        checkArgument(positionCount > 0, "positionCount should be > 0, but is %s", positionCount);

        Block mergeRow = inputPage.getBlock(mergeRowChannel).getLoadedBlock();
        if (mergeRow.mayHaveNull()) {
            for (int position = 0; position < positionCount; position++) {
                checkArgument(!mergeRow.isNull(position), "The mergeRow may not have null rows");
            }
        }

        List<Block> fields = getRowFieldsFromBlock(mergeRow);
        List<Block> builder = new ArrayList<>(dataColumnChannels.size() + 3);
        for (int channel : dataColumnChannels) {
            builder.add(fields.get(channel));
        }
        Block operationChannelBlock = fields.get(fields.size() - 2);
        builder.add(operationChannelBlock);
        builder.add(inputPage.getBlock(rowIdChannel));
        builder.add(RunLengthEncodedBlock.create(INSERT_FROM_UPDATE_BLOCK, positionCount));

        Page result = new Page(builder.toArray(Block[]::new));

        int defaultCaseCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (TINYINT.getByte(operationChannelBlock, position) == DEFAULT_CASE_OPERATION_NUMBER) {
                defaultCaseCount++;
            }
        }
        if (defaultCaseCount == 0) {
            return result;
        }

        int usedCases = 0;
        int[] positions = new int[positionCount - defaultCaseCount];
        for (int position = 0; position < positionCount; position++) {
            if (TINYINT.getByte(operationChannelBlock, position) != DEFAULT_CASE_OPERATION_NUMBER) {
                positions[usedCases] = position;
                usedCases++;
            }
        }

        checkArgument(usedCases + defaultCaseCount == positionCount, "usedCases (%s) + defaultCaseCount (%s) != positionCount (%s)", usedCases, defaultCaseCount, positionCount);

        return result.getPositions(positions, 0, usedCases);
    }
}
