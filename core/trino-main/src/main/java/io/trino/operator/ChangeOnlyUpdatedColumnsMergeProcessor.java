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
import io.trino.spi.block.ColumnarRow;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.RowChangeParadigm;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static io.trino.spi.block.ColumnarRow.toColumnarRow;
import static io.trino.spi.connector.RowChangeParadigm.CHANGE_ONLY_UPDATED_COLUMNS;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ChangeOnlyUpdatedColumnsMergeProcessor
        implements MergeRowChangeProcessor
{
    private static final Block INSERT_FROM_UPDATE_BLOCK = nativeValueToBlock(TINYINT, 0L);

    private final int writeRedistributionColumnCount;
    private final int rowIdChannel;
    private final int mergeRowChannel;
    private final List<Integer> dataColumnChannels;

    public ChangeOnlyUpdatedColumnsMergeProcessor(
            List<Type> dataColumnTypes,
            Type rowIdType,
            int rowIdChannel,
            int mergeRowChannel,
            List<Integer> redistributionColumnChannels,
            List<Integer> dataColumnChannels)
    {
        requireNonNull(dataColumnTypes, "dataColumnTypes is null");
        this.writeRedistributionColumnCount = redistributionColumnChannels.size();
        requireNonNull(rowIdType, "rowIdType is null");
        this.rowIdChannel = rowIdChannel;
        this.mergeRowChannel = mergeRowChannel;
        this.dataColumnChannels = requireNonNull(dataColumnChannels, "dataColumnChannels is null");
    }

    @Override
    public RowChangeParadigm getRowChangeParadigm()
    {
        return CHANGE_ONLY_UPDATED_COLUMNS;
    }

    @Override
    public Page transformPage(Page inputPage)
    {
        requireNonNull(inputPage, "inputPage is null");
        int inputChannelCount = inputPage.getChannelCount();
        if (inputChannelCount < 2 + writeRedistributionColumnCount) {
            throw new IllegalArgumentException(format("inputPage channelCount (%s) should be >= 2 + %s", inputChannelCount, writeRedistributionColumnCount));
        }

        int positionCount = inputPage.getPositionCount();
        if (positionCount <= 0) {
            throw new IllegalArgumentException("positionCount should be > 0, but is " + positionCount);
        }

        ColumnarRow mergeRow = toColumnarRow(inputPage.getBlock(mergeRowChannel));
        Block operationChannelBlock = mergeRow.getField(mergeRow.getFieldCount() - 2);

        List<Block> builder = new ArrayList<>(dataColumnChannels.size() + 3);

        for (int channel : dataColumnChannels) {
            builder.add(mergeRow.getField(channel));
        }
        builder.add(operationChannelBlock);
        builder.add(inputPage.getBlock(rowIdChannel));
        builder.add(new RunLengthEncodedBlock(INSERT_FROM_UPDATE_BLOCK, positionCount));

        Page result = new Page(builder.toArray(Block[]::new));

        int defaultCaseCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (TINYINT.getLong(operationChannelBlock, position) == DEFAULT_CASE_OPERATION_NUMBER) {
                defaultCaseCount++;
            }
        }
        if (defaultCaseCount == 0) {
            return result;
        }

        int usedCases = 0;
        int[] positions = new int[positionCount - defaultCaseCount];
        for (int position = 0; position < positionCount; position++) {
            if (TINYINT.getLong(operationChannelBlock, position) != DEFAULT_CASE_OPERATION_NUMBER) {
                positions[usedCases] = position;
                usedCases++;
            }
        }

        verify(usedCases + defaultCaseCount == positionCount, "usedCases (%s) + defaultCaseCount (%s) != positionCount (%s)", usedCases, defaultCaseCount, positionCount);

        return result.getPositions(positions, 0, usedCases);
    }
}
