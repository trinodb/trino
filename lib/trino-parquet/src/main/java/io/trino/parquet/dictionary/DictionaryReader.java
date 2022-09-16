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
package io.trino.parquet.dictionary;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;

public class DictionaryReader
        extends ValuesReader
{
    private final Dictionary dictionary;
    private RunLengthBitPackingHybridDecoder decoder;

    public DictionaryReader(Dictionary dictionary)
    {
        this.dictionary = dictionary;
    }

    @Override
    public void initFromPage(int valueCount, ByteBufferInputStream in)
            throws IOException
    {
        int bitWidth = BytesUtils.readIntLittleEndianOnOneByte(in);
        decoder = new RunLengthBitPackingHybridDecoder(bitWidth, in);
    }

    @Override
    public int readValueDictionaryId()
    {
        return readInt();
    }

    @Override
    public Binary readBytes()
    {
        return dictionary.decodeToBinary(readInt());
    }

    @Override
    public float readFloat()
    {
        return dictionary.decodeToFloat(readInt());
    }

    @Override
    public double readDouble()
    {
        return dictionary.decodeToDouble(readInt());
    }

    @Override
    public int readInteger()
    {
        return dictionary.decodeToInt(readInt());
    }

    // Read array of ints and convert from indices to values
    @Override
    public void readIntegers(int[] arr, int offset, int len)
    {
        readInts(arr, offset, len);
        dictionary.decodeToInts(arr, offset, len);
    }

    @Override
    public long readLong()
    {
        return dictionary.decodeToLong(readInt());
    }

    // Temporary array to hold indices.
    // This must not be static, because that would not be thread-safe with other
    // DictionaryReaders in other threads. The maximum size of this array will be
    // that of the maximum rows in a Parquet page, which is only 1024 in my
    // experience, so the performance benefit to holding onto this across multiple
    // batch read calls will outweigh the memory burden.
    int[] intArr;

    // Read array of ints and convert from indices to long values
    @Override
    public void readLongs(long[] arr, int offset, int len)
    {
        if (intArr == null || intArr.length < offset + len) {
            intArr = new int[offset + len];
        }

        readInts(intArr, offset, len);
        dictionary.decodeToLongs(intArr, arr, offset, len);
    }

    // Read array of ints and convert from indices to float values
    @Override
    public void readFloats(float[] arr, int offset, int len)
    {
        if (intArr == null || intArr.length < offset + len) {
            intArr = new int[offset + len];
        }

        readInts(intArr, offset, len);
        dictionary.decodeToFloats(intArr, arr, offset, len);
    }

    // Read array of ints and convert from indices to double values
    @Override
    public void readDoubles(double[] arr, int offset, int len)
    {
        if (intArr == null || intArr.length < offset + len) {
            intArr = new int[offset + len];
        }

        readInts(intArr, offset, len);
        dictionary.decodeToDoubles(intArr, arr, offset, len);
    }

    @Override
    public void skip()
    {
        try {
            decoder.skip();
        }
        catch (IOException e) {
            throw new ParquetDecodingException(e);
        }
    }

    @Override
    public void skip(int n)
    {
        try {
            decoder.skip(n);
        }
        catch (IOException e) {
            throw new ParquetDecodingException(e);
        }
    }

    private int readInt()
    {
        try {
            return decoder.readInt();
        }
        catch (IOException e) {
            throw new ParquetDecodingException(e);
        }
    }

    // Fetch an array of indices
    private void readInts(int[] arr, int offset, int len)
    {
        try {
            decoder.readInts(arr, offset, len);
        }
        catch (IOException e) {
            throw new ParquetDecodingException(e);
        }
    }
}
