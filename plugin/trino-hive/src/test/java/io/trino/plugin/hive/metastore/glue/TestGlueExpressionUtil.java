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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.metastore.Column;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.metastore.glue.GlueExpressionUtil.buildGlueExpression;
import static io.trino.plugin.hive.metastore.glue.GlueExpressionUtil.buildGlueExpressionForSingleDomain;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class TestGlueExpressionUtil
{
    private static Column getColumn(String name, String type)
    {
        return new Column(name, HiveType.valueOf(type), Optional.empty());
    }

    @Test
    public void testBuildGlueExpressionDomainEqualsSingleValue()
    {
        Domain domain = Domain.singleValue(VarcharType.VARCHAR, utf8Slice("2020-01-01"));
        Optional<String> foo = buildGlueExpressionForSingleDomain("foo", domain, true);
        assertEquals(foo.get(), "((foo = '2020-01-01'))");
    }

    @Test
    public void testBuildGlueExpressionTupleDomainEqualsSingleValue()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addStringValues("col1", "2020-01-01")
                .addStringValues("col2", "2020-02-20")
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1", "col2"), filter, true);
        assertEquals(expression, "((col1 = '2020-01-01')) AND ((col2 = '2020-02-20'))");
    }

    @Test
    public void testBuildGlueExpressionTupleDomainEqualsAndInClause()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addStringValues("col1", "2020-01-01")
                .addStringValues("col2", "2020-02-20", "2020-02-28")
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1", "col2"), filter, true);
        assertEquals(expression, "((col1 = '2020-01-01')) AND ((col2 in ('2020-02-20', '2020-02-28')))");
    }

    @Test
    public void testBuildGlueExpressionTupleDomainExtraDomain()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addStringValues("col1", "2020-01-01")
                .addStringValues("col2", "2020-02-20", "2020-02-28")
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1"), filter, true);
        assertEquals(expression, "((col1 = '2020-01-01'))");
    }

    @Test
    public void testBuildGlueExpressionTupleDomainRange()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addStringValues("col1", "2020-01-01")
                .addRanges("col2", Range.greaterThan(BIGINT, 100L))
                .addRanges("col2", Range.lessThan(BIGINT, 0L))
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1", "col2"), filter, true);
        assertEquals(expression, "((col1 = '2020-01-01')) AND ((col2 < 0) OR (col2 > 100))");
    }

    @Test
    public void testBuildGlueExpressionTupleDomainEqualAndRangeLong()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addBigintValues("col1", 3L)
                .addRanges("col1", Range.greaterThan(BIGINT, 100L))
                .addRanges("col1", Range.lessThan(BIGINT, 0L))
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1"), filter, true);
        assertEquals(expression, "((col1 < 0) OR (col1 > 100) OR (col1 = 3))");
    }

    @Test
    public void testBuildGlueExpressionTupleDomainEqualAndRangeString()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addStringValues("col1", "2020-01-01", "2020-01-31")
                .addRanges("col1", Range.range(VarcharType.VARCHAR, utf8Slice("2020-03-01"), true, utf8Slice("2020-03-31"), true))
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1"), filter, true);
        assertEquals(expression, "((col1 >= '2020-03-01' AND col1 <= '2020-03-31') OR (col1 in ('2020-01-01', '2020-01-31')))");
    }

    @Test
    public void testBuildGlueExpressionExtraColumn()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addStringValues("col1", "2020-01-01")
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1", "col2"), filter, true);
        assertEquals(expression, "((col1 = '2020-01-01'))");
    }

    @Test
    public void testBuildGlueExpressionTupleDomainIsNull()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addDomain("col1", Domain.onlyNull(VarcharType.VARCHAR))
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1"), filter, true);
        assertEquals(expression, format("(col1 = '%s')", GlueExpressionUtil.NULL_STRING));
    }

    @Test
    public void testBuildGlueExpressionTupleDomainNotNull()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addDomain("col1", Domain.notNull(VarcharType.VARCHAR))
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1"), filter, true);
        assertEquals(expression, format("(col1 <> '%s')", GlueExpressionUtil.NULL_STRING));
    }

    @Test
    public void testBuildGlueExpressionTupleDomainEqualsOrIsNull()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addStringValues("col1", "2020-01-01")
                .addDomain("col1", Domain.onlyNull(VarcharType.VARCHAR))
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1"), filter, true);
        assertEquals(expression, format("((col1 = '2020-01-01') OR (col1 = '%s'))", GlueExpressionUtil.NULL_STRING));
    }

    @Test
    public void testBuildGlueExpressionTupleDomainEqualsAndIsNotNull()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addStringValues("col1", "2020-01-01")
                .addDomain("col2", Domain.notNull(VarcharType.VARCHAR))
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1", "col2"), filter, true);
        assertEquals(expression, format("((col1 = '2020-01-01')) AND (col2 <> '%s')", GlueExpressionUtil.NULL_STRING));
    }

    @Test
    public void testBuildGlueExpressionMaxLengthNone()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addStringValues("col1", "x".repeat(101))
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1"), filter, true, 100);
        assertEquals(expression, "");
    }

    @Test
    public void testBuildGlueExpressionMaxLengthOneColumn()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addStringValues("col1", "x".repeat(5))
                .addStringValues("col2", "x".repeat(25))
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1", "col2"), filter, true, 20);
        assertEquals(expression, "((col1 = 'xxxxx'))");
    }

    @Test
    public void testBuildGlueExpressionTupleDomainAll()
    {
        String expression = buildGlueExpression(ImmutableList.of("col1"), TupleDomain.all(), true);
        assertEquals(expression, "");
    }

    @Test
    public void testDecimalConverstion()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addDecimalValues("col1", "10.134")
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1"), filter, true);
        assertEquals(expression, "((col1 = 10.13400))");
    }

    @Test
    public void testBigintConversion()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addBigintValues("col1", Long.MAX_VALUE)
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1"), filter, true);
        assertEquals(expression, format("((col1 = %d))", Long.MAX_VALUE));
    }

    @Test
    public void testIntegerConversion()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addIntegerValues("col1", Long.valueOf(Integer.MAX_VALUE))
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1"), filter, true);
        assertEquals(expression, format("((col1 = %d))", Integer.MAX_VALUE));
    }

    @Test
    public void testSmallintConversion()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addIntegerValues("col1", Long.valueOf(Short.MAX_VALUE))
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1"), filter, true);
        assertEquals(expression, format("((col1 = %d))", Short.MAX_VALUE));
    }

    @Test
    public void testTinyintConversion()
    {
        TupleDomain<String> filter = new PartitionFilterBuilder()
                .addIntegerValues("col1", Long.valueOf(Byte.MAX_VALUE))
                .build();
        String expression = buildGlueExpression(ImmutableList.of("col1"), filter, true);
        assertEquals(expression, format("((col1 = %d))", Byte.MAX_VALUE));
    }
}
