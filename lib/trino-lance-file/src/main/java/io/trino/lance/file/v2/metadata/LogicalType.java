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
package io.trino.lance.file.v2.metadata;

import com.google.common.base.Splitter;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public sealed interface LogicalType
        permits
        LogicalType.Int8Type,
        LogicalType.Int16Type,
        LogicalType.Int32Type,
        LogicalType.Int64Type,
        LogicalType.FloatType,
        LogicalType.DoubleType,
        LogicalType.StringType,
        LogicalType.BinaryType,
        LogicalType.FixedSizeListType,
        LogicalType.ListType,
        LogicalType.StructType,
        LogicalType.DateType
{
    static LogicalType from(String type)
    {
        requireNonNull(type, "type is null");
        checkArgument(!type.isEmpty(), "type is empty");
        List<String> components = Splitter.on(':').splitToList(type.toUpperCase(ENGLISH));
        LogicalTypeKind kind = LogicalTypeKind.valueOf(components.getFirst());
        return switch (kind) {
            case INT8 -> Int8Type.INT8_TYPE;
            case INT16 -> Int16Type.INT16_TYPE;
            case INT32 -> Int32Type.INT32_TYPE;
            case INT64 -> Int64Type.INT64_TYPE;
            case FLOAT -> FloatType.FLOAT_TYPE;
            case DOUBLE -> DoubleType.DOUBLE_TYPE;
            case STRING -> StringType.STRING_TYPE;
            case BINARY -> BinaryType.BINARY_TYPE;
            case FIXED_SIZE_LIST -> {
                checkArgument(components.size() == 3, "FixedSizeList type signature must have exactly 3 components");
                int size = Integer.parseInt(components.getLast());
                LogicalTypeKind dataType = LogicalTypeKind.valueOf(components.get(1));
                yield new FixedSizeListType(dataType, size);
            }
            case LIST -> new ListType();
            case STRUCT -> new StructType();
            case DATE32 -> {
                checkArgument(components.size() == 2, "DATE32 signature must have exactly 2 components");
                checkArgument(components.get(1).toLowerCase(ENGLISH).equals("day"), "only supports date32:day");
                yield new DateType();
            }
        };
    }

    enum LogicalTypeKind
    {
        INT8,
        INT16,
        INT32,
        INT64,
        FLOAT,
        DOUBLE,
        STRING,
        BINARY,
        FIXED_SIZE_LIST,
        LIST,
        STRUCT,
        DATE32
    }

    record Int8Type()
            implements LogicalType
    {
        public static final Int8Type INT8_TYPE = new Int8Type();
    }

    record Int16Type()
            implements LogicalType
    {
        public static final Int16Type INT16_TYPE = new Int16Type();
    }

    record Int32Type()
            implements LogicalType
    {
        public static final Int32Type INT32_TYPE = new Int32Type();
    }

    record Int64Type()
            implements LogicalType
    {
        public static final Int64Type INT64_TYPE = new Int64Type();
    }

    record FloatType()
            implements LogicalType
    {
        public static final FloatType FLOAT_TYPE = new FloatType();
    }

    record DoubleType()
            implements LogicalType
    {
        public static final DoubleType DOUBLE_TYPE = new DoubleType();
    }

    record StringType()
            implements LogicalType
    {
        public static final StringType STRING_TYPE = new StringType();
    }

    record BinaryType()
            implements LogicalType
    {
        public static final BinaryType BINARY_TYPE = new BinaryType();
    }

    record FixedSizeListType(LogicalTypeKind kind, int size)
            implements LogicalType {}

    record ListType()
            implements LogicalType {}

    record StructType()
            implements LogicalType {}

    record DateType()
            implements LogicalType {}
}
