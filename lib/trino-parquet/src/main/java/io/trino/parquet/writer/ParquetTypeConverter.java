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
package io.trino.parquet.writer;

import org.apache.parquet.format.Type;
import org.apache.parquet.schema.PrimitiveType;

// Copy from parquet-mr
public class ParquetTypeConverter
{
    private ParquetTypeConverter() {}

    public static org.apache.parquet.format.Type getType(PrimitiveType.PrimitiveTypeName type)
    {
        return switch (type) {
            case INT64 -> Type.INT64;
            case INT32 -> Type.INT32;
            case BOOLEAN -> Type.BOOLEAN;
            case BINARY -> Type.BYTE_ARRAY;
            case FLOAT -> Type.FLOAT;
            case DOUBLE -> Type.DOUBLE;
            case INT96 -> Type.INT96;
            case FIXED_LEN_BYTE_ARRAY -> Type.FIXED_LEN_BYTE_ARRAY;
        };
    }
}
