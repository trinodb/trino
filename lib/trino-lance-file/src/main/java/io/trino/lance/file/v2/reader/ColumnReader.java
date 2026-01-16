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
package io.trino.lance.file.v2.reader;

import io.trino.lance.file.LanceDataSource;
import io.trino.lance.file.v2.metadata.ColumnMetadata;
import io.trino.lance.file.v2.metadata.Field;
import io.trino.lance.file.v2.metadata.LogicalType;
import io.trino.memory.context.AggregatedMemoryContext;

import java.util.List;
import java.util.Map;

public interface ColumnReader
{
    static ColumnReader createColumnReader(LanceDataSource dataSource,
            Field field,
            Map<Integer, ColumnMetadata> columnMetadata,
            List<Range> readRanges,
            AggregatedMemoryContext memoryContext)
    {
        return switch (LogicalType.from(field.logicalType())) {
            case LogicalType.Int8Type _,
                 LogicalType.Int16Type _,
                 LogicalType.Int32Type _,
                 LogicalType.Int64Type _,
                 LogicalType.FloatType _,
                 LogicalType.DoubleType _,
                 LogicalType.StringType _,
                 LogicalType.BinaryType _,
                 LogicalType.DateType _ -> new PrimitiveColumnReader(dataSource, field, columnMetadata.get(field.id()), readRanges, memoryContext);
            case LogicalType.ListType _ -> new ListColumnReader(dataSource, field, columnMetadata, readRanges, memoryContext);
            case LogicalType.StructType _ -> new StructColumnReader(dataSource, field, columnMetadata, readRanges, memoryContext);
            default -> throw new RuntimeException("Unsupported logical type: " + field.logicalType());
        };
    }

    void prepareNextRead(int batchSize);

    DecodedPage read();
}
