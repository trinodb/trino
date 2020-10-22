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
package io.prestosql.orc.reader;

import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcBlockFactory;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.OrcReader;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.Type;

import static io.prestosql.orc.metadata.OrcType.OrcTypeKind.LONG;
import static io.prestosql.orc.reader.ReaderUtils.invalidStreamType;
import static io.prestosql.spi.type.TimeType.TIME_MICROS;

public final class ColumnReaders
{
    private ColumnReaders() {}

    public static ColumnReader createColumnReader(
            Type type,
            OrcColumn column,
            OrcReader.ProjectedLayout projectedLayout,
            AggregatedMemoryContext systemMemoryContext,
            OrcBlockFactory blockFactory)
            throws OrcCorruptionException
    {
        if (type instanceof TimeType) {
            if (!type.equals(TIME_MICROS) || column.getColumnType() != LONG ||
                    !"TIME".equals(column.getAttributes().get("iceberg.long-type"))) {
                throw invalidStreamType(column, type);
            }
            return new TimeColumnReader(type, column, systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
        }

        switch (column.getColumnType()) {
            case BOOLEAN:
                return new BooleanColumnReader(type, column, systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case BYTE:
                return new ByteColumnReader(type, column, systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case SHORT:
            case INT:
            case LONG:
            case DATE:
                return new LongColumnReader(type, column, systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case FLOAT:
                return new FloatColumnReader(type, column, systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case DOUBLE:
                return new DoubleColumnReader(type, column, systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case BINARY:
            case STRING:
            case VARCHAR:
            case CHAR:
                return new SliceColumnReader(type, column, systemMemoryContext);
            case TIMESTAMP:
            case TIMESTAMP_INSTANT:
                return new TimestampColumnReader(type, column, systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case LIST:
                return new ListColumnReader(type, column, systemMemoryContext, blockFactory);
            case STRUCT:
                return new StructColumnReader(type, column, projectedLayout, systemMemoryContext, blockFactory);
            case MAP:
                return new MapColumnReader(type, column, systemMemoryContext, blockFactory);
            case DECIMAL:
                return new DecimalColumnReader(type, column, systemMemoryContext.newLocalMemoryContext(ColumnReaders.class.getSimpleName()));
            case UNION:
                return new UnionColumnReader(type, column, systemMemoryContext, blockFactory);
            default:
                throw new IllegalArgumentException("Unsupported type: " + column.getColumnType());
        }
    }
}
