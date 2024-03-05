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

import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.flat.FlatColumnReader;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.Test;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestColumnReaderFactory
{
    @Test
    public void testTopLevelPrimitiveFields()
    {
        ColumnReaderFactory columnReaderFactory = new ColumnReaderFactory(UTC);
        PrimitiveType primitiveType = new PrimitiveType(OPTIONAL, INT32, "test");

        PrimitiveField topLevelRepeatedPrimitiveField = new PrimitiveField(
                INTEGER,
                true,
                new ColumnDescriptor(new String[] {"topLevelRepeatedPrimitiveField test"}, primitiveType, 1, 1),
                0);
        assertThat(columnReaderFactory.create(topLevelRepeatedPrimitiveField, newSimpleAggregatedMemoryContext())).isInstanceOf(NestedColumnReader.class);

        PrimitiveField topLevelOptionalPrimitiveField = new PrimitiveField(
                INTEGER,
                false,
                new ColumnDescriptor(new String[] {"topLevelRequiredPrimitiveField test"}, primitiveType, 0, 1),
                0);
        assertThat(columnReaderFactory.create(topLevelOptionalPrimitiveField, newSimpleAggregatedMemoryContext())).isInstanceOf(FlatColumnReader.class);

        PrimitiveField topLevelRequiredPrimitiveField = new PrimitiveField(
                INTEGER,
                true,
                new ColumnDescriptor(new String[] {"topLevelRequiredPrimitiveField test"}, primitiveType, 0, 0),
                0);
        assertThat(columnReaderFactory.create(topLevelRequiredPrimitiveField, newSimpleAggregatedMemoryContext())).isInstanceOf(FlatColumnReader.class);
    }
}
