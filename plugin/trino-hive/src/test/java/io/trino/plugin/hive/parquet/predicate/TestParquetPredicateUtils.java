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
package io.trino.plugin.hive.parquet.predicate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HiveType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeOperators;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.parquet.ParquetTypeUtils.getDescriptors;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.getParquetTupleDomain;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.rowType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestParquetPredicateUtils
{
    @Test(dataProvider = "useColumnNames")
    public void testParquetTupleDomainPrimitiveArray(boolean useColumnNames)
    {
        HiveColumnHandle columnHandle = createBaseColumn("my_array", 0, HiveType.valueOf("array<int>"), new ArrayType(INTEGER), REGULAR, Optional.empty());
        TupleDomain<HiveColumnHandle> domain = withColumnDomains(ImmutableMap.of(columnHandle, Domain.notNull(new ArrayType(INTEGER))));

        MessageType fileSchema = new MessageType("hive_schema",
                new GroupType(OPTIONAL, "my_array",
                        new GroupType(REPEATED, "bag", new PrimitiveType(OPTIONAL, INT32, "array_element"))));

        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> tupleDomain = getParquetTupleDomain(descriptorsByPath, domain, fileSchema, useColumnNames);
        assertTrue(tupleDomain.isAll());
    }

    @Test(dataProvider = "useColumnNames")
    public void testParquetTupleDomainStructArray(boolean useColumnNames)
    {
        RowType.Field rowField = new RowType.Field(Optional.of("a"), INTEGER);
        RowType rowType = RowType.from(ImmutableList.of(rowField));

        HiveColumnHandle columnHandle = createBaseColumn("my_array_struct", 0, HiveType.valueOf("array<struct<a:int>>"), rowType, REGULAR, Optional.empty());

        TupleDomain<HiveColumnHandle> domain = withColumnDomains(ImmutableMap.of(columnHandle, Domain.notNull(new ArrayType(rowType))));

        MessageType fileSchema = new MessageType("hive_schema",
                new GroupType(OPTIONAL, "my_array_struct",
                        new GroupType(REPEATED, "bag",
                                new GroupType(OPTIONAL, "array_element", new PrimitiveType(OPTIONAL, INT32, "a")))));

        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> tupleDomain = getParquetTupleDomain(descriptorsByPath, domain, fileSchema, useColumnNames);
        assertTrue(tupleDomain.isAll());
    }

    @Test(dataProvider = "useColumnNames")
    public void testParquetTupleDomainPrimitive(boolean useColumnNames)
    {
        HiveColumnHandle columnHandle = createBaseColumn("my_primitive", 0, HiveType.valueOf("bigint"), BIGINT, REGULAR, Optional.empty());
        Domain singleValueDomain = Domain.singleValue(BIGINT, 123L);
        TupleDomain<HiveColumnHandle> domain = withColumnDomains(ImmutableMap.of(columnHandle, singleValueDomain));

        MessageType fileSchema = new MessageType("hive_schema", new PrimitiveType(OPTIONAL, INT64, "my_primitive"));

        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> tupleDomain = getParquetTupleDomain(descriptorsByPath, domain, fileSchema, useColumnNames);

        assertEquals(tupleDomain.getDomains().get().size(), 1);
        ColumnDescriptor descriptor = tupleDomain.getDomains().get().keySet().iterator().next();
        assertEquals(descriptor.getPath().length, 1);
        assertEquals(descriptor.getPath()[0], "my_primitive");

        Domain predicateDomain = Iterables.getOnlyElement(tupleDomain.getDomains().get().values());
        assertEquals(predicateDomain, singleValueDomain);
    }

    @Test(dataProvider = "useColumnNames")
    public void testParquetTupleDomainStruct(boolean useColumnNames)
    {
        RowType rowType = rowType(
                RowType.field("a", INTEGER),
                RowType.field("b", INTEGER));

        HiveColumnHandle columnHandle = createBaseColumn("my_struct", 0, HiveType.valueOf("struct<a:int,b:int>"), rowType, REGULAR, Optional.empty());
        TupleDomain<HiveColumnHandle> domain = withColumnDomains(ImmutableMap.of(columnHandle, Domain.notNull(rowType)));

        MessageType fileSchema = new MessageType("hive_schema",
                new GroupType(OPTIONAL, "my_struct",
                        new PrimitiveType(OPTIONAL, INT32, "a"),
                        new PrimitiveType(OPTIONAL, INT32, "b"),
                        new PrimitiveType(OPTIONAL, INT32, "c")));
        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> tupleDomain = getParquetTupleDomain(descriptorsByPath, domain, fileSchema, useColumnNames);
        assertTrue(tupleDomain.isAll());
    }

    @Test(dataProvider = "useColumnNames")
    public void testParquetTupleDomainStructWithPrimitiveColumnPredicate(boolean useColumNames)
    {
        RowType baseType = rowType(
                RowType.field("a", INTEGER),
                RowType.field("b", INTEGER),
                RowType.field("c", INTEGER));

        HiveColumnProjectionInfo columnProjectionInfo = new HiveColumnProjectionInfo(
                ImmutableList.of(1),
                ImmutableList.of("b"),
                HiveType.HIVE_INT,
                INTEGER);

        HiveColumnHandle projectedColumn = new HiveColumnHandle(
                "row_field",
                0,
                HiveType.toHiveType(baseType),
                baseType,
                Optional.of(columnProjectionInfo),
                REGULAR,
                Optional.empty());

        Domain predicateDomain = Domain.singleValue(INTEGER, 123L);
        TupleDomain<HiveColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(projectedColumn, predicateDomain));

        MessageType fileSchema = new MessageType("hive_schema",
                new GroupType(OPTIONAL, "row_field",
                        new PrimitiveType(OPTIONAL, INT32, "a"),
                        new PrimitiveType(OPTIONAL, INT32, "b"),
                        new PrimitiveType(OPTIONAL, INT32, "c")));
        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> calculatedTupleDomain = getParquetTupleDomain(descriptorsByPath, tupleDomain, fileSchema, useColumNames);
        assertEquals(calculatedTupleDomain.getDomains().get().size(), 1);
        ColumnDescriptor selectedColumnDescriptor = descriptorsByPath.get(ImmutableList.of("row_field", "b"));
        assertEquals(calculatedTupleDomain.getDomains().get().get(selectedColumnDescriptor), predicateDomain);
    }

    @Test(dataProvider = "useColumnNames")
    public void testParquetTupleDomainStructWithComplexColumnPredicate(boolean useColumNames)
    {
        RowType c1Type = rowType(
                RowType.field("c1", INTEGER),
                RowType.field("c2", INTEGER));
        RowType baseType = rowType(
                RowType.field("a", INTEGER),
                RowType.field("b", INTEGER),
                RowType.field("c", c1Type));

        HiveColumnProjectionInfo columnProjectionInfo = new HiveColumnProjectionInfo(
                ImmutableList.of(2),
                ImmutableList.of("C"),
                HiveType.toHiveType(c1Type),
                c1Type);

        HiveColumnHandle projectedColumn = new HiveColumnHandle(
                "row_field",
                0,
                HiveType.toHiveType(baseType),
                baseType,
                Optional.of(columnProjectionInfo),
                REGULAR,
                Optional.empty());

        Domain predicateDomain = Domain.onlyNull(c1Type);
        TupleDomain<HiveColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(projectedColumn, predicateDomain));

        MessageType fileSchema = new MessageType("hive_schema",
                new GroupType(OPTIONAL, "row_field",
                        new PrimitiveType(OPTIONAL, INT32, "a"),
                        new PrimitiveType(OPTIONAL, INT32, "b"),
                        new GroupType(OPTIONAL,
                                "c",
                                new PrimitiveType(OPTIONAL, INT32, "c1"),
                                new PrimitiveType(OPTIONAL, INT32, "c2"))));
        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        // skip looking up predicates for complex types as Parquet only stores stats for primitives
        TupleDomain<ColumnDescriptor> calculatedTupleDomain = getParquetTupleDomain(descriptorsByPath, tupleDomain, fileSchema, useColumNames);
        assertTrue(calculatedTupleDomain.isAll());
    }

    @Test(dataProvider = "useColumnNames")
    public void testParquetTupleDomainStructWithMissingPrimitiveColumn(boolean useColumnNames)
    {
        RowType baseType = rowType(
                RowType.field("a", INTEGER),
                RowType.field("b", INTEGER),
                RowType.field("non_exist", INTEGER));

        HiveColumnProjectionInfo columnProjectionInfo = new HiveColumnProjectionInfo(
                ImmutableList.of(2),
                ImmutableList.of("non_exist"),
                HiveType.HIVE_INT,
                INTEGER);

        HiveColumnHandle projectedColumn = new HiveColumnHandle(
                "row_field",
                0,
                HiveType.toHiveType(baseType),
                baseType,
                Optional.of(columnProjectionInfo),
                REGULAR,
                Optional.empty());

        Domain predicateDomain = Domain.singleValue(INTEGER, 123L);
        TupleDomain<HiveColumnHandle> tupleDomain = withColumnDomains(ImmutableMap.of(projectedColumn, predicateDomain));

        MessageType fileSchema = new MessageType("hive_schema",
                new GroupType(OPTIONAL, "row_field",
                        new PrimitiveType(OPTIONAL, INT32, "a"),
                        new PrimitiveType(OPTIONAL, INT32, "b")));
        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> calculatedTupleDomain = getParquetTupleDomain(descriptorsByPath, tupleDomain, fileSchema, useColumnNames);
        assertTrue(calculatedTupleDomain.isAll());
    }

    @Test(dataProvider = "useColumnNames")
    public void testParquetTupleDomainMap(boolean useColumnNames)
    {
        MapType mapType = new MapType(INTEGER, INTEGER, new TypeOperators());

        HiveColumnHandle columnHandle = createBaseColumn("my_map", 0, HiveType.valueOf("map<int,int>"), mapType, REGULAR, Optional.empty());

        TupleDomain<HiveColumnHandle> domain = withColumnDomains(ImmutableMap.of(columnHandle, Domain.notNull(mapType)));

        MessageType fileSchema = new MessageType("hive_schema",
                new GroupType(OPTIONAL, "my_map",
                        new GroupType(REPEATED, "map",
                                new PrimitiveType(REQUIRED, INT32, "key"),
                                new PrimitiveType(OPTIONAL, INT32, "value"))));

        Map<List<String>, ColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, fileSchema);
        TupleDomain<ColumnDescriptor> tupleDomain = getParquetTupleDomain(descriptorsByPath, domain, fileSchema, useColumnNames);
        assertTrue(tupleDomain.isAll());
    }

    @DataProvider
    public Object[][] useColumnNames()
    {
        return new Object[][] {
                {true}, // use column name
                {false} // use column index
        };
    }
}
