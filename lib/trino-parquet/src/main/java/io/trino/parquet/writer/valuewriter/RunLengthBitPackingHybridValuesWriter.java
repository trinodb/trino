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

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.ParquetEncodingException;

import java.io.IOException;

import static java.lang.Math.toIntExact;
import static org.apache.parquet.column.Encoding.RLE;

public class RunLengthBitPackingHybridValuesWriter
        implements ColumnDescriptorValuesWriter
{
    private final RunLengthBitPackingHybridEncoder encoder;

    public RunLengthBitPackingHybridValuesWriter(int bitWidth, int maxCapacityHint)
    {
        this.encoder = new RunLengthBitPackingHybridEncoder(bitWidth, maxCapacityHint);
    }

    @Override
    public void writeInteger(int value)
    {
        try {
            encoder.writeInt(value);
        }
        catch (IOException e) {
            throw new ParquetEncodingException(e);
        }
    }

    @Override
    public void writeRepeatInteger(int value, int valueRepetitions)
    {
        try {
            encoder.writeRepeatedInteger(value, valueRepetitions);
        }
        catch (IOException e) {
            throw new ParquetEncodingException(e);
        }
    }

    @Override
    public long getBufferedSize()
    {
        return encoder.getBufferedSize();
    }

    @Override
    public long getAllocatedSize()
    {
        return encoder.getAllocatedSize();
    }

    @Override
    public BytesInput getBytes()
    {
        try {
            // prepend the length of the column
            BytesInput rle = encoder.toBytes();
            return BytesInput.concat(BytesInput.fromInt(toIntExact(rle.size())), rle);
        }
        catch (IOException e) {
            throw new ParquetEncodingException(e);
        }
    }

    @Override
    public Encoding getEncoding()
    {
        return RLE;
    }

    @Override
    public void reset()
    {
        encoder.reset();
    }
}
