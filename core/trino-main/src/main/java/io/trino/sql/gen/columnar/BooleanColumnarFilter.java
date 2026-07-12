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
package io.trino.sql.gen.columnar;

import io.trino.operator.project.InputChannels;
import io.trino.spi.block.BitArrayBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;

import static io.trino.spi.block.Bitmap.getBits;
import static io.trino.spi.block.Bitmap.isSet;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class BooleanColumnarFilter
        implements ColumnarFilter
{
    private final InputChannels inputChannels;

    public BooleanColumnarFilter(InputChannels inputChannels)
    {
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
    }

    @Override
    public InputChannels getInputChannels()
    {
        return inputChannels;
    }

    protected boolean isNegated()
    {
        return false;
    }

    @Override
    public int filterPositionsRange(ConnectorSession session, int[] outputPositions, int offset, int size, SourcePage page)
    {
        BitArrayBlock block = (BitArrayBlock) page.getBlock(0);
        long[] values = block.getRawValues();
        long[] validity = block.getRawValueIsValid();
        int rawOffset = block.getRawValuesOffset();

        int outputCount = 0;
        int end = offset + size;
        for (int position = offset; position < end; position += Long.SIZE) {
            int bitsInWord = min(Long.SIZE, end - position);
            long selected = getBits(values, rawOffset, position, bitsInWord);
            if (isNegated()) {
                selected = ~selected;
            }
            if (validity != null) {
                selected &= getBits(validity, rawOffset, position, bitsInWord);
            }
            while (selected != 0) {
                int bit = Long.numberOfTrailingZeros(selected);
                if (bit >= bitsInWord) {
                    break;
                }
                outputPositions[outputCount++] = position + bit;
                selected &= selected - 1;
            }
        }
        return outputCount;
    }

    @Override
    public int filterPositionsList(ConnectorSession session, int[] outputPositions, int[] activePositions, int offset, int size, SourcePage page)
    {
        BitArrayBlock block = (BitArrayBlock) page.getBlock(0);
        long[] values = block.getRawValues();
        long[] validity = block.getRawValueIsValid();
        int rawOffset = block.getRawValuesOffset();

        int outputCount = 0;
        for (int index = offset; index < offset + size; index++) {
            int position = activePositions[index];
            boolean selected = isSet(values, rawOffset, position) != isNegated();
            if (validity != null) {
                selected &= isSet(validity, rawOffset, position);
            }
            outputPositions[outputCount] = position;
            outputCount += selected ? 1 : 0;
        }
        return outputCount;
    }

    public static final class Negated
            extends BooleanColumnarFilter
    {
        public Negated(InputChannels inputChannels)
        {
            super(inputChannels);
        }

        @Override
        protected boolean isNegated()
        {
            return true;
        }
    }
}
