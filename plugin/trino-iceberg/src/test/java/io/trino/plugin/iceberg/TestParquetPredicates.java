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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.STRUCT;
import static io.trino.plugin.iceberg.IcebergPageSourceProvider.getParquetTupleDomain;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.rowType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetPredicates
{
    @Test
    public void testParquetTupleDomainStructWithPrimitiveColumnPredicate()
    {
        // trino type
        RowType baseType = rowType(
                RowType.field("a", INTEGER),
                RowType.field("b", INTEGER),
                RowType.field("c", INTEGER));

        // iceberg type
        ColumnIdentity fieldA = new ColumnIdentity(1, "a", PRIMITIVE, ImmutableList.of());
        ColumnIdentity fieldB = new ColumnIdentity(2, "b", PRIMITIVE, ImmutableList.of());
        ColumnIdentity fieldC = new ColumnIdentity(3, "c", PRIMITIVE, ImmutableList.of());

        // parquet type
        MessageType fileSchema = new MessageType("iceberg_schema",
                new GroupType(OPTIONAL, "row_field",
                        new PrimitiveType(OPTIONAL, INT32, "a").withId(1),
                        new PrimitiveType(OPTIONAL, INT32, "b").withId(2),
                        new PrimitiveType(OPTIONAL, INT32, "c").withId(3)));

        // predicate domain
        IcebergColumnHandle projectedColumn = new IcebergColumnHandle(
                new ColumnIdentity(
                        5,
                        "row_field",
                        STRUCT,
                        ImmutableList.of(fieldA, fieldB, fieldC)),
                baseType,
                ImmutableList.of(2),
                INTEGER,
                false,
                Optional.empty());
        Domain predicateDomain = Domain.singleValue(INTEGER, 123L);
        TupleDomain<IcebergColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(projectedColumn, predicateDomain));

        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> calculatedTupleDomain = getParquetTupleDomain(descriptorsByPath, tupleDomain);

        assertThat(calculatedTupleDomain.getDomains().orElseThrow().size()).isEqualTo(1);
        ColumnDescriptor selectedColumnDescriptor = descriptorsByPath.get(ImmutableList.of("row_field", "b"));
        assertThat(calculatedTupleDomain.getDomains().orElseThrow().get(selectedColumnDescriptor)).isEqualTo(predicateDomain);
    }

    @Test
    public void testParquetTupleDomainStructWithPrimitiveColumnDifferentIdPredicate()
    {
        // trino type
        RowType baseType = rowType(
                RowType.field("a", INTEGER),
                RowType.field("b", INTEGER),
                RowType.field("c", INTEGER));

        // iceberg type
        ColumnIdentity fieldA = new ColumnIdentity(1, "a", PRIMITIVE, ImmutableList.of());
        ColumnIdentity fieldB = new ColumnIdentity(2, "b", PRIMITIVE, ImmutableList.of());
        ColumnIdentity fieldC = new ColumnIdentity(4, "c", PRIMITIVE, ImmutableList.of());

        // parquet type
        MessageType fileSchema = new MessageType("iceberg_schema",
                new GroupType(OPTIONAL, "row_field",
                        new PrimitiveType(OPTIONAL, INT32, "a").withId(1),
                        new PrimitiveType(OPTIONAL, INT32, "b").withId(2),
                        new PrimitiveType(OPTIONAL, INT32, "c").withId(3)).withId(5));

        // predicate domain
        IcebergColumnHandle projectedColumn = new IcebergColumnHandle(
                new ColumnIdentity(
                        5,
                        "row_field",
                        STRUCT,
                        ImmutableList.of(fieldA, fieldB, fieldC)),
                baseType,
                ImmutableList.of(4),
                INTEGER,
                false,
                Optional.empty());
        Domain predicateDomain = Domain.singleValue(INTEGER, 123L);
        TupleDomain<IcebergColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(projectedColumn, predicateDomain));

        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> calculatedTupleDomain = getParquetTupleDomain(descriptorsByPath, tupleDomain);
        // same name but different Id between iceberg and parquet for field c
        assertThat(calculatedTupleDomain.isAll()).isTrue();
    }

    @Test
    public void testParquetTupleDomainStructWithComplexColumnPredicate()
    {
        // trino type
        RowType nestedType = rowType(
                RowType.field("c1", INTEGER),
                RowType.field("c2", INTEGER));
        RowType baseType = rowType(
                RowType.field("a", INTEGER),
                RowType.field("b", INTEGER),
                RowType.field("c", nestedType));

        // iceberg type
        ColumnIdentity fieldC11 = new ColumnIdentity(1, "c1", PRIMITIVE, ImmutableList.of());
        ColumnIdentity fieldC12 = new ColumnIdentity(2, "c2", PRIMITIVE, ImmutableList.of());
        ColumnIdentity fieldA = new ColumnIdentity(3, "a", PRIMITIVE, ImmutableList.of());
        ColumnIdentity fieldB = new ColumnIdentity(4, "b", PRIMITIVE, ImmutableList.of());
        ColumnIdentity fieldC = new ColumnIdentity(5, "c", STRUCT, ImmutableList.of(fieldC11, fieldC12));

        // parquet type
        MessageType fileSchema = new MessageType("iceberg_schema",
                new GroupType(OPTIONAL, "row_field",
                        new PrimitiveType(OPTIONAL, INT32, "a").withId(3),
                        new PrimitiveType(OPTIONAL, INT32, "b").withId(4),
                        new GroupType(OPTIONAL,
                                "c",
                                new PrimitiveType(OPTIONAL, INT32, "c1").withId(1),
                                new PrimitiveType(OPTIONAL, INT32, "c2").withId(2)).withId(5)));
        // predicate domain
        IcebergColumnHandle projectedColumn = new IcebergColumnHandle(
                new ColumnIdentity(
                        6,
                        "row_field",
                        STRUCT,
                        ImmutableList.of(fieldA, fieldB, fieldC)),
                baseType,
                ImmutableList.of(5),
                nestedType,
                false,
                Optional.empty());

        Domain predicateDomain = Domain.onlyNull(nestedType);
        TupleDomain<IcebergColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(projectedColumn, predicateDomain));

        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> calculatedTupleDomain = getParquetTupleDomain(descriptorsByPath, tupleDomain);

        assertThat(calculatedTupleDomain.isAll()).isTrue();
    }

    @Test
    public void testParquetTupleDomainStructWithMissingPrimitiveColumn()
    {
        // trino type
        RowType baseType = rowType(
                RowType.field("a", INTEGER),
                RowType.field("b", INTEGER),
                RowType.field("missing", INTEGER));

        // iceberg type
        ColumnIdentity fieldA = new ColumnIdentity(1, "a", PRIMITIVE, ImmutableList.of());
        ColumnIdentity fieldB = new ColumnIdentity(2, "b", PRIMITIVE, ImmutableList.of());
        ColumnIdentity fieldC = new ColumnIdentity(3, "missing", PRIMITIVE, ImmutableList.of());

        // parquet type
        MessageType fileSchema = new MessageType("iceberg_schema",
                new GroupType(OPTIONAL, "row_field",
                        new PrimitiveType(OPTIONAL, INT32, "a").withId(1),
                        new PrimitiveType(OPTIONAL, INT32, "b").withId(2)));

        // predicate domain
        IcebergColumnHandle projectedColumn = new IcebergColumnHandle(
                new ColumnIdentity(
                        5,
                        "row_field",
                        STRUCT,
                        ImmutableList.of(fieldA, fieldB, fieldC)),
                baseType,
                ImmutableList.of(3),
                INTEGER,
                false,
                Optional.empty());
        Domain predicateDomain = Domain.singleValue(INTEGER, 123L);
        TupleDomain<IcebergColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(projectedColumn, predicateDomain));

        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> calculatedTupleDomain = getParquetTupleDomain(descriptorsByPath, tupleDomain);

        assertThat(calculatedTupleDomain.isAll()).isTrue();
    }
}
