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
import io.trino.orc.OrcBlockFactory;
import io.trino.orc.OrcColumn;
import io.trino.orc.OrcCorruptionException;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReader.FieldMapperFactory;
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
            OrcBlockFactory blockFactory,
            FieldMapperFactory fieldMapperFactory)
            throws OrcCorruptionException
    {
        if (type instanceof TimeType) {
            if (!type.equals(TIME_MICROS) || column.getColumnType() != LONG ||
                    !"TIME".equals(column.getAttributes().get(ICEBERG_LONG_TYPE))) {
                throw invalidStreamType(column, type);
            }
            return new TimeColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
        }
        if (type instanceof UuidType) {
            checkArgument(column.getColumnType() == BINARY, "UUID type can only be read from BINARY column but got " + column);
            checkArgument(
                    "UUID".equals(column.getAttributes().get(ICEBERG_BINARY_TYPE)),
                    "Expected ORC column for UUID data to be annotated with %s=UUID: %s",
                    ICEBERG_BINARY_TYPE, column);
            return new UuidColumnReader(column);
        }

        switch (column.getColumnType()) {
            case BOOLEAN:
                return new BooleanColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case BYTE:
                if (type == INTEGER && !column.getAttributes().containsKey("iceberg.id")) {
                    throw invalidStreamType(column, type);
                }
                return new ByteColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case SHORT:
            case INT:
            case LONG:
            case DATE:
                return new LongColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case FLOAT:
                return new FloatColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case DOUBLE:
                return new DoubleColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case BINARY:
            case STRING:
            case VARCHAR:
            case CHAR:
                return new SliceColumnReader(type, column, memoryContext);
            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
                return new TimestampColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case LIST:
                return new ListColumnReader(type, column, memoryContext, blockFactory, fieldMapperFactory);
            case STRUCT:
                return new StructColumnReader(type, column, projectedLayout, memoryContext, blockFactory, fieldMapperFactory);
            case MAP:
                return new MapColumnReader(type, column, memoryContext, blockFactory, fieldMapperFactory);
            case DECIMAL:
                return new DecimalColumnReader(type, column, memoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case UNION:
                return new UnionColumnReader(type, column, memoryContext, blockFactory, fieldMapperFactory);
        }
        throw new IllegalArgumentException("Unsupported type: " + column.getColumnType());
    }
}
