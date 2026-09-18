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
package io.trino.parquet.reader;

import io.trino.parquet.ParquetEncoding;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainFloatDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter.PlainLongDictionaryValuesWriter;
import org.apache.parquet.column.values.plain.BooleanPlainValuesWriter;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.util.OptionalInt;

import static io.trino.parquet.ParquetEncoding.DELTA_BINARY_PACKED;
import static io.trino.parquet.ParquetEncoding.DELTA_BYTE_ARRAY;
import static io.trino.parquet.ParquetEncoding.DELTA_LENGTH_BYTE_ARRAY;
import static io.trino.parquet.ParquetEncoding.PLAIN;
import static io.trino.parquet.ParquetEncoding.PLAIN_DICTIONARY;
import static io.trino.parquet.ParquetEncoding.RLE;
import static io.trino.parquet.ParquetEncoding.RLE_DICTIONARY;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.format;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;

public final class TestingValuesWriters
{
    private static final int MAX_DATA_SIZE = 1_000_000;

    private TestingValuesWriters() {}

    public static ValuesWriter getValuesWriter(ParquetEncoding encoding, PrimitiveTypeName typeName, OptionalInt typeLength)
    {
        if (encoding.equals(RLE)) {
            if (typeName.equals(BOOLEAN)) {
                return new RunLengthBitPackingHybridValuesWriter(1, MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
            }
            throw new IllegalArgumentException("RLE encoding writer is not supported for type " + typeName);
        }
        if (encoding.equals(PLAIN)) {
            return switch (typeName) {
                case BOOLEAN -> new BooleanPlainValuesWriter();
                case FIXED_LEN_BYTE_ARRAY -> new FixedLenByteArrayPlainValuesWriter(typeLength.orElseThrow(), MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
                case BINARY, INT32, INT64, DOUBLE, FLOAT -> new PlainValuesWriter(MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
                case INT96 -> new FixedLenByteArrayPlainValuesWriter(12, MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
            };
        }
        if (encoding.equals(RLE_DICTIONARY) || encoding.equals(PLAIN_DICTIONARY)) {
            return switch (typeName) {
                case BINARY -> new PlainBinaryDictionaryValuesWriter(MAX_VALUE, Encoding.RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
                case FIXED_LEN_BYTE_ARRAY -> new PlainFixedLenArrayDictionaryValuesWriter(MAX_VALUE, typeLength.orElseThrow(), Encoding.RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
                case INT32 -> new PlainIntegerDictionaryValuesWriter(MAX_VALUE, Encoding.RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
                case INT64 -> new PlainLongDictionaryValuesWriter(MAX_VALUE, Encoding.RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
                case FLOAT -> new PlainFloatDictionaryValuesWriter(MAX_VALUE, Encoding.RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
                case DOUBLE -> new PlainDoubleDictionaryValuesWriter(MAX_VALUE, Encoding.RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
                case INT96 -> new PlainFixedLenArrayDictionaryValuesWriter(MAX_VALUE, 12, Encoding.RLE, Encoding.PLAIN, HeapByteBufferAllocator.getInstance());
                default -> throw new IllegalArgumentException("Dictionary encoding writer is not supported for type " + typeName);
            };
        }
        if (encoding.equals(DELTA_BINARY_PACKED)) {
            return switch (typeName) {
                case INT32 -> new DeltaBinaryPackingValuesWriterForInteger(MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
                case INT64 -> new DeltaBinaryPackingValuesWriterForLong(MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
                default -> throw new IllegalArgumentException("Delta binary packing encoding writer is not supported for type " + typeName);
            };
        }
        if (encoding.equals(DELTA_LENGTH_BYTE_ARRAY)) {
            if (typeName.equals(BINARY)) {
                return new DeltaLengthByteArrayValuesWriter(MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
            }
            throw new IllegalArgumentException("Delta length byte array encoding writer is not supported for type " + typeName);
        }
        if (encoding.equals(DELTA_BYTE_ARRAY)) {
            if (typeName.equals(BINARY) || typeName.equals(FIXED_LEN_BYTE_ARRAY)) {
                return new DeltaByteArrayWriter(MAX_DATA_SIZE, MAX_DATA_SIZE, HeapByteBufferAllocator.getInstance());
            }
            throw new IllegalArgumentException("Delta byte array encoding writer is not supported for type " + typeName);
        }
        throw new UnsupportedOperationException(format("Encoding %s is not supported", encoding));
    }
}
