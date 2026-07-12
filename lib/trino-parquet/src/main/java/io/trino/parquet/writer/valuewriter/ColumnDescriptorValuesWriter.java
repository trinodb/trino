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
package io.trino.parquet.writer.valuewriter;

import io.trino.spi.block.Bitmap;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;

import static org.apache.parquet.bytes.BytesUtils.getWidthFromMaxInt;

/**
 * Used for writing repetition and definition levels
 */
public interface ColumnDescriptorValuesWriter
{
    /**
     * @param value the value to encode
     */
    void writeInteger(int value);

    /**
     * @param value the value to encode
     * @param valueRepetitions number of times the input value is repeated in the input stream
     */
    void writeRepeatInteger(int value, int valueRepetitions);

    default int writeBitmap(Bitmap bitmap, int offset, int positionCount, int setValue, int unsetValue)
    {
        int setCount = 0;
        int position = 0;
        while (position < positionCount) {
            int bitsInWord = Math.min(Long.SIZE, positionCount - position);
            long bits = Bitmap.getBits(bitmap.getRawWords(), bitmap.getRawBitOffset(), offset + position, bitsInWord);
            int positionInWord = 0;
            while (positionInWord < bitsInWord) {
                boolean set = (bits & 1) != 0;
                int runLength = Math.min(Long.numberOfTrailingZeros(set ? ~bits : bits), bitsInWord - positionInWord);
                writeRepeatInteger(set ? setValue : unsetValue, runLength);
                if (set) {
                    setCount += runLength;
                }
                bits >>>= runLength;
                positionInWord += runLength;
            }
            position += bitsInWord;
        }
        return setCount;
    }

    /**
     * used to decide if we want to work to the next page
     *
     * @return the size of the currently buffered data (in bytes)
     */
    long getBufferedSize();

    /**
     * @return the allocated size of the buffer
     */
    long getAllocatedSize();

    /**
     * @return the bytes buffered so far to write to the current page
     */
    BytesInput getBytes();

    /**
     * @return the encoding that was used to encode the bytes
     */
    Encoding getEncoding();

    /**
     * called after getBytes() to reset the current buffer and start writing the next page
     */
    void reset();

    static ColumnDescriptorValuesWriter newRepetitionLevelWriter(ColumnDescriptor path, int pageSizeThreshold)
    {
        return newColumnDescriptorValuesWriter(path.getMaxRepetitionLevel(), pageSizeThreshold);
    }

    static ColumnDescriptorValuesWriter newDefinitionLevelWriter(ColumnDescriptor path, int pageSizeThreshold)
    {
        return newColumnDescriptorValuesWriter(path.getMaxDefinitionLevel(), pageSizeThreshold);
    }

    private static ColumnDescriptorValuesWriter newColumnDescriptorValuesWriter(int maxLevel, int pageSizeThreshold)
    {
        if (maxLevel == 0) {
            return new DevNullValuesWriter();
        }
        return new RunLengthBitPackingHybridValuesWriter(getWidthFromMaxInt(maxLevel), pageSizeThreshold);
    }
}
