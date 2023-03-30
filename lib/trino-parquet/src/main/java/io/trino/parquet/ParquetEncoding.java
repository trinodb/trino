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
package io.trino.parquet;

import io.trino.parquet.dictionary.BinaryDictionary;
import io.trino.parquet.dictionary.Dictionary;
import io.trino.parquet.dictionary.DictionaryReader;
import io.trino.parquet.dictionary.DoubleDictionary;
import io.trino.parquet.dictionary.FloatDictionary;
import io.trino.parquet.dictionary.IntegerDictionary;
import io.trino.parquet.dictionary.LongDictionary;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.ByteBitPackingValuesReader;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesReader;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayReader;
import org.apache.parquet.column.values.plain.BinaryPlainValuesReader;
import org.apache.parquet.column.values.plain.BooleanPlainValuesReader;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader.DoublePlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader.FloatPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader.IntegerPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesReader.LongPlainValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesReader;
import org.apache.parquet.column.values.rle.ZeroIntegerValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.parquet.column.values.bitpacking.Packer.BIG_ENDIAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public enum ParquetEncoding
{
    PLAIN {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType)
        {
            return switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
                case BOOLEAN -> new BooleanPlainValuesReader();
                case BINARY -> new BinaryPlainValuesReader();
                case FLOAT -> new FloatPlainValuesReader();
                case DOUBLE -> new DoublePlainValuesReader();
                case INT32 -> new IntegerPlainValuesReader();
                case INT64 -> new LongPlainValuesReader();
                case INT96 -> new FixedLenByteArrayPlainValuesReader(INT96_TYPE_LENGTH);
                case FIXED_LEN_BYTE_ARRAY -> new FixedLenByteArrayPlainValuesReader(descriptor.getPrimitiveType().getTypeLength());
            };
        }

        @Override
        public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage)
                throws IOException
        {
            return switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
                // No dictionary encoding for boolean
                case BOOLEAN -> throw new ParquetDecodingException("Dictionary encoding does not support: " + descriptor.getPrimitiveType().getPrimitiveTypeName());
                case BINARY -> new BinaryDictionary(dictionaryPage);
                case FIXED_LEN_BYTE_ARRAY -> new BinaryDictionary(dictionaryPage, descriptor.getPrimitiveType().getTypeLength());
                case INT96 -> new BinaryDictionary(dictionaryPage, INT96_TYPE_LENGTH);
                case INT64 -> new LongDictionary(dictionaryPage);
                case DOUBLE -> new DoubleDictionary(dictionaryPage);
                case INT32 -> new IntegerDictionary(dictionaryPage);
                case FLOAT -> new FloatDictionary(dictionaryPage);
            };
        }
    },

    RLE {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType)
        {
            int bitWidth = BytesUtils.getWidthFromMaxInt(getMaxLevel(descriptor, valuesType));
            if (bitWidth == 0) {
                return new ZeroIntegerValuesReader();
            }
            return new RunLengthBitPackingHybridValuesReader(bitWidth);
        }
    },

    BIT_PACKED {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType)
        {
            return new ByteBitPackingValuesReader(getMaxLevel(descriptor, valuesType), BIG_ENDIAN);
        }
    },

    PLAIN_DICTIONARY {
        @Override
        public ValuesReader getDictionaryBasedValuesReader(ColumnDescriptor descriptor, ValuesType valuesType, Dictionary dictionary)
        {
            return RLE_DICTIONARY.getDictionaryBasedValuesReader(descriptor, valuesType, dictionary);
        }

        @Override
        public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage)
                throws IOException
        {
            return PLAIN.initDictionary(descriptor, dictionaryPage);
        }

        @Override
        public boolean usesDictionary()
        {
            return true;
        }
    },

    DELTA_BINARY_PACKED {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType)
        {
            PrimitiveTypeName typeName = descriptor.getPrimitiveType().getPrimitiveTypeName();
            checkArgument(typeName == INT32 || typeName == INT64, "Encoding DELTA_BINARY_PACKED is only supported for type INT32 and INT64");
            return new DeltaBinaryPackingValuesReader();
        }
    },

    DELTA_LENGTH_BYTE_ARRAY {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType)
        {
            checkArgument(descriptor.getPrimitiveType().getPrimitiveTypeName() == BINARY, "Encoding DELTA_LENGTH_BYTE_ARRAY is only supported for type BINARY");
            return new DeltaLengthByteArrayValuesReader();
        }
    },

    DELTA_BYTE_ARRAY {
        @Override
        public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType)
        {
            PrimitiveTypeName typeName = descriptor.getPrimitiveType().getPrimitiveTypeName();
            checkArgument(typeName == BINARY || typeName == FIXED_LEN_BYTE_ARRAY, "Encoding DELTA_BYTE_ARRAY is only supported for type BINARY and FIXED_LEN_BYTE_ARRAY");
            return new DeltaByteArrayReader();
        }
    },

    RLE_DICTIONARY {
        @Override
        public ValuesReader getDictionaryBasedValuesReader(ColumnDescriptor descriptor, ValuesType valuesType, Dictionary dictionary)
        {
            return new DictionaryReader(dictionary);
        }

        @Override
        public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage)
                throws IOException
        {
            return PLAIN.initDictionary(descriptor, dictionaryPage);
        }

        @Override
        public boolean usesDictionary()
        {
            return true;
        }
    };

    static final int INT96_TYPE_LENGTH = 12;

    static int getMaxLevel(ColumnDescriptor descriptor, ValuesType valuesType)
    {
        return switch (valuesType) {
            case REPETITION_LEVEL -> descriptor.getMaxRepetitionLevel();
            case DEFINITION_LEVEL -> descriptor.getMaxDefinitionLevel();
            case VALUES -> {
                if (descriptor.getPrimitiveType().getPrimitiveTypeName() == BOOLEAN) {
                    yield 1;
                }
                throw new ParquetDecodingException("Unsupported values type: " + valuesType);
            }
        };
    }

    public boolean usesDictionary()
    {
        return false;
    }

    public Dictionary initDictionary(ColumnDescriptor descriptor, DictionaryPage dictionaryPage)
            throws IOException
    {
        throw new UnsupportedOperationException(" Dictionary encoding is not supported for: " + name());
    }

    public ValuesReader getValuesReader(ColumnDescriptor descriptor, ValuesType valuesType)
    {
        throw new UnsupportedOperationException("Error decoding  values in encoding: " + this.name());
    }

    public ValuesReader getDictionaryBasedValuesReader(ColumnDescriptor descriptor, ValuesType valuesType, Dictionary dictionary)
    {
        throw new UnsupportedOperationException(" Dictionary encoding is not supported for: " + name());
    }
}
