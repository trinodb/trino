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
package io.trino.orc.reader;

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReader.FieldMapperFactory;
import io.trino.orc.metadata.OrcType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.BINARY;
import static io.trino.orc.metadata.OrcType.OrcTypeKind.LONG;
import static io.trino.orc.reader.ReaderUtils.invalidStreamType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeType.TIME_MICROS;

public final class ColumnReaders
{
    public static final String ICEBERG_BINARY_TYPE = "iceberg.binary-type";
    public static final String ICEBERG_LONG_TYPE = "iceberg.long-type";

    private ColumnReaders() {}

    public static ColumnReader createColumnReader(
            Type type,
            OrcColumn column,
            OrcReader.ProjectedLayout projectedLayout,
            AggregatedMemoryContext memoryContext,
            FieldMapperFactory fieldMapperFactory)
            throws OrcCorruptionException
    {
        OrcType.OrcTypeKind orcTypeKind = column.getColumnType().getOrcTypeKind();
        if (type instanceof TimeType) {
            if (!type.equals(TIME_MICROS) || orcTypeKind != LONG ||
                    !"TIME".equals(column.getAttributes().get(ICEBERG_LONG_TYPE))) {
                throw invalidStreamType(column, type);
            }
            return new TimeColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
        }
        if (type instanceof UuidType) {
            checkArgument(orcTypeKind == BINARY, "UUID type can only be read from BINARY column but got %s", column);
            checkArgument(
                    "UUID".equals(column.getAttributes().get(ICEBERG_BINARY_TYPE)),
                    "Expected ORC column for UUID data to be annotated with %s=UUID: %s",
                    ICEBERG_BINARY_TYPE, column);
            return new UuidColumnReader(column);
        }

        return switch (orcTypeKind) {
            case BOOLEAN -> new BooleanColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case BYTE -> {
                if (type == INTEGER && !column.getAttributes().containsKey("iceberg.id")) {
                    throw invalidStreamType(column, type);
                }
                yield new ByteColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            }
            case SHORT, INT, LONG, DATE -> new LongColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case FLOAT -> new FloatColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case DOUBLE -> new DoubleColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case BINARY, STRING, VARCHAR, CHAR -> new SliceColumnReader(type, column, memoryContext);
            case TIMESTAMP, TIMESTAMP_INSTANT -> new TimestampColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case LIST -> new ListColumnReader(type, column, memoryContext, fieldMapperFactory);
            case STRUCT -> new StructColumnReader(type, column, projectedLayout, memoryContext, fieldMapperFactory);
            case MAP -> new MapColumnReader(type, column, memoryContext, fieldMapperFactory);
            case DECIMAL -> new DecimalColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case UNION -> new UnionColumnReader(type, column, memoryContext, fieldMapperFactory);
        };
    }
}
