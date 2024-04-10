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
package io.trino.plugin.hive.parquet;

import io.trino.plugin.hive.coercions.IntegerNumberToDoubleCoercer;
import io.trino.plugin.hive.coercions.TypeCoercer;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.schema.LogicalTypeAnnotation;

import java.util.Optional;

import static io.trino.parquet.reader.ColumnReaderFactory.isIntegerAnnotationAndPrimitive;
import static io.trino.plugin.hive.coercions.TimestampCoercer.LongTimestampToVarcharCoercer;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

public final class ParquetTypeTranslator
{
    private ParquetTypeTranslator() {}

    public static Optional<TypeCoercer<? extends Type, ? extends Type>> createCoercer(PrimitiveTypeName fromParquetType, LogicalTypeAnnotation typeAnnotation, Type toTrinoType)
    {
        if (toTrinoType instanceof DoubleType) {
            if (isIntegerAnnotationAndPrimitive(typeAnnotation, fromParquetType)) {
                if (fromParquetType == INT32) {
                    return Optional.of(new IntegerNumberToDoubleCoercer<>(INTEGER));
                }
                if (fromParquetType == INT64) {
                    return Optional.of(new IntegerNumberToDoubleCoercer<>(BIGINT));
                }
            }
        }
        if (toTrinoType instanceof VarcharType varcharType) {
            if (fromParquetType == INT96) {
                return Optional.of(new LongTimestampToVarcharCoercer(TIMESTAMP_NANOS, varcharType));
            }
        }
        return Optional.empty();
    }
}
