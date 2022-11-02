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
package io.trino.operator.aggregation.minmaxbyn;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static io.trino.block.BlockAssertions.createArrayBigintBlock;
import static io.trino.block.BlockAssertions.createBlockOfReals;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createRepeatedValuesBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.operator.aggregation.AggregationTestUtils.groupedAggregation;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertEquals;

public class TestMinMaxByNAggregation
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    @Test
    public void testMaxDoubleDouble()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(DOUBLE, DOUBLE, BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                Arrays.asList((Double) null),
                createDoublesBlock(1.0, null),
                createDoublesBlock(3.0, 5.0),
                createRepeatedValuesBlock(1L, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                null,
                createDoublesBlock(null, null),
                createDoublesBlock(null, null),
                createRepeatedValuesBlock(1L, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                Arrays.asList(1.0),
                createDoublesBlock(null, 1.0, null, null),
                createDoublesBlock(null, 0.0, null, null),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                Arrays.asList(1.0),
                createDoublesBlock(1.0),
                createDoublesBlock(0.0),
                createRepeatedValuesBlock(2L, 1));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                null,
                createDoublesBlock(),
                createDoublesBlock(),
                createRepeatedValuesBlock(2L, 0));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of(2.5),
                createDoublesBlock(2.5, 2.0, 5.0, 3.0),
                createDoublesBlock(4.0, 1.5, 2.0, 3.0),
                createRepeatedValuesBlock(1L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of(2.5, 3.0),
                createDoublesBlock(2.5, 2.0, 5.0, 3.0),
                createDoublesBlock(4.0, 1.5, 2.0, 3.0),
                createRepeatedValuesBlock(2L, 4));
    }

    @Test
    public void testMinDoubleDouble()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(DOUBLE, DOUBLE, BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                Arrays.asList((Double) null),
                createDoublesBlock(1.0, null),
                createDoublesBlock(5.0, 3.0),
                createRepeatedValuesBlock(1L, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                null,
                createDoublesBlock(null, null),
                createDoublesBlock(null, null),
                createRepeatedValuesBlock(1L, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of(2.0),
                createDoublesBlock(2.5, 2.0, 5.0, 3.0),
                createDoublesBlock(4.0, 1.5, 2.0, 3.0),
                createRepeatedValuesBlock(1L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of(2.0, 5.0),
                createDoublesBlock(2.5, 2.0, 5.0, 3.0),
                createDoublesBlock(4.0, 1.5, 2.0, 3.0),
                createRepeatedValuesBlock(2L, 4));
    }

    @Test
    public void testMinDoubleVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, DOUBLE, BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("z", "a"),
                createStringsBlock("z", "a", "x", "b"),
                createDoublesBlock(1.0, 2.0, 2.0, 3.0),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("a", "zz"),
                createStringsBlock("zz", "hi", "bb", "a"),
                createDoublesBlock(0.0, 1.0, 2.0, -1.0),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("a", "zz"),
                createStringsBlock("zz", "hi", null, "a"),
                createDoublesBlock(0.0, 1.0, null, -1.0),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("b", "c"),
                createStringsBlock("a", "b", "c", "d"),
                createDoublesBlock(Double.NaN, 2.0, 3.0, 4.0),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("a", "c"),
                createStringsBlock("a", "b", "c", "d"),
                createDoublesBlock(1.0, Double.NaN, 3.0, 4.0),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("a", "b"),
                createStringsBlock("a", "b", "c", "d"),
                createDoublesBlock(1.0, 2.0, Double.NaN, 4.0),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("a", "b"),
                createStringsBlock("a", "b", "c", "d"),
                createDoublesBlock(1.0, 2.0, 3.0, Double.NaN),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("a", "b"),
                createStringsBlock("a", "b"),
                createDoublesBlock(1.0, Double.NaN),
                createRepeatedValuesBlock(2L, 2));
    }

    @Test
    public void testMaxDoubleVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, DOUBLE, BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("a", "z"),
                createStringsBlock("z", "a", null),
                createDoublesBlock(1.0, 2.0, null),
                createRepeatedValuesBlock(2L, 3));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("bb", "hi"),
                createStringsBlock("zz", "hi", "bb", "a"),
                createDoublesBlock(0.0, 1.0, 2.0, -1.0),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("hi", "zz"),
                createStringsBlock("zz", "hi", null, "a"),
                createDoublesBlock(0.0, 1.0, null, -1.0),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("d", "c"),
                createStringsBlock("a", "b", "c", "d"),
                createDoublesBlock(Double.NaN, 2.0, 3.0, 4.0),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("d", "c"),
                createStringsBlock("a", "b", "c", "d"),
                createDoublesBlock(1.0, Double.NaN, 3.0, 4.0),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("d", "b"),
                createStringsBlock("a", "b", "c", "d"),
                createDoublesBlock(1.0, 2.0, Double.NaN, 4.0),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("c", "b"),
                createStringsBlock("a", "b", "c", "d"),
                createDoublesBlock(1.0, 2.0, 3.0, Double.NaN),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("a", "b"),
                createStringsBlock("a", "b"),
                createDoublesBlock(1.0, Double.NaN),
                createRepeatedValuesBlock(2L, 2));
    }

    @Test
    public void testMinRealVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, REAL, BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("z", "a"),
                createStringsBlock("z", "a", "x", "b"),
                createBlockOfReals(1.0f, 2.0f, 2.0f, 3.0f),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("a", "zz"),
                createStringsBlock("zz", "hi", "bb", "a"),
                createBlockOfReals(0.0f, 1.0f, 2.0f, -1.0f),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("a", "zz"),
                createStringsBlock("zz", "hi", null, "a"),
                createBlockOfReals(0.0f, 1.0f, null, -1.0f),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("b", "c"),
                createStringsBlock("a", "b", "c", "d"),
                createBlockOfReals(Float.NaN, 2.0f, 3.0f, 4.0f),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("a", "c"),
                createStringsBlock("a", "b", "c", "d"),
                createBlockOfReals(1.0f, Float.NaN, 3.0f, 4.0f),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("a", "b"),
                createStringsBlock("a", "b", "c", "d"),
                createBlockOfReals(1.0f, 2.0f, Float.NaN, 4.0f),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("a", "b"),
                createStringsBlock("a", "b", "c", "d"),
                createBlockOfReals(1.0f, 2.0f, 3.0f, Float.NaN),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("a", "b"),
                createStringsBlock("a", "b"),
                createBlockOfReals(1.0f, Float.NaN),
                createRepeatedValuesBlock(2L, 2));
    }

    @Test
    public void testMaxRealVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, REAL, BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("a", "z"),
                createStringsBlock("z", "a", null),
                createBlockOfReals(1.0f, 2.0f, null),
                createRepeatedValuesBlock(2L, 3));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("bb", "hi"),
                createStringsBlock("zz", "hi", "bb", "a"),
                createBlockOfReals(0.0f, 1.0f, 2.0f, -1.0f),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("hi", "zz"),
                createStringsBlock("zz", "hi", null, "a"),
                createBlockOfReals(0.0f, 1.0f, null, -1.0f),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("d", "c"),
                createStringsBlock("a", "b", "c", "d"),
                createBlockOfReals(Float.NaN, 2.0f, 3.0f, 4.0f),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("d", "c"),
                createStringsBlock("a", "b", "c", "d"),
                createBlockOfReals(1.0f, Float.NaN, 3.0f, 4.0f),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("d", "b"),
                createStringsBlock("a", "b", "c", "d"),
                createBlockOfReals(1.0f, 2.0f, Float.NaN, 4.0f),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("c", "b"),
                createStringsBlock("a", "b", "c", "d"),
                createBlockOfReals(1.0f, 2.0f, 3.0f, Float.NaN),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("a", "b"),
                createStringsBlock("a", "b"),
                createBlockOfReals(1.0f, Float.NaN),
                createRepeatedValuesBlock(2L, 2));
    }

    @Test
    public void testMinVarcharDouble()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(DOUBLE, VARCHAR, BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of(2.0, 3.0),
                createDoublesBlock(1.0, 2.0, 2.0, 3.0),
                createStringsBlock("z", "a", "x", "b"),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of(-1.0, 2.0),
                createDoublesBlock(0.0, 1.0, 2.0, -1.0),
                createStringsBlock("zz", "hi", "bb", "a"),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of(-1.0, 1.0),
                createDoublesBlock(0.0, 1.0, null, -1.0),
                createStringsBlock("zz", "hi", null, "a"),
                createRepeatedValuesBlock(2L, 4));
    }

    @Test
    public void testMaxVarcharDouble()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(DOUBLE, VARCHAR, BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of(1.0, 2.0),
                createDoublesBlock(1.0, 2.0, null),
                createStringsBlock("z", "a", null),
                createRepeatedValuesBlock(2L, 3));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of(0.0, 1.0),
                createDoublesBlock(0.0, 1.0, 2.0, -1.0),
                createStringsBlock("zz", "hi", "bb", "a"),
                createRepeatedValuesBlock(2L, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of(0.0, 1.0),
                createDoublesBlock(0.0, 1.0, null, -1.0),
                createStringsBlock("zz", "hi", null, "a"),
                createRepeatedValuesBlock(2L, 4));
    }

    @Test
    public void testMinVarcharArray()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(new ArrayType(BIGINT), VARCHAR, BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of(ImmutableList.of(2L, 3L), ImmutableList.of(4L, 5L)),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(2L, 3L), ImmutableList.of(3L, 4L), ImmutableList.of(4L, 5L))),
                createStringsBlock("z", "a", "x", "b"),
                createRepeatedValuesBlock(2L, 4));
    }

    @Test
    public void testMaxVarcharArray()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(new ArrayType(BIGINT), VARCHAR, BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(3L, 4L)),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(2L, 3L), ImmutableList.of(3L, 4L), ImmutableList.of(4L, 5L))),
                createStringsBlock("z", "a", "x", "b"),
                createRepeatedValuesBlock(2L, 4));
    }

    @Test
    public void testMinArrayVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, new ArrayType(BIGINT), BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of("b", "x", "z"),
                createStringsBlock("z", "a", "x", "b"),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(2L, 3L), ImmutableList.of(0L, 3L), ImmutableList.of(0L, 2L))),
                createRepeatedValuesBlock(3L, 4));
    }

    @Test
    public void testMaxArrayVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, new ArrayType(BIGINT), BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of("a", "z", "x"),
                createStringsBlock("z", "a", "x", "b"),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(1L, 2L), ImmutableList.of(2L, 3L), ImmutableList.of(0L, 3L), ImmutableList.of(0L, 2L))),
                createRepeatedValuesBlock(3L, 4));
    }

    @Test
    public void testOutOfBound()
    {
        TestingAggregationFunction function = FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("max_by"), fromTypes(VARCHAR, BIGINT, BIGINT));
        try {
            groupedAggregation(function, new Page(createStringsBlock("z"), createLongsBlock(0), createLongsBlock(10001)));
        }
        catch (TrinoException e) {
            assertEquals(e.getMessage(), "third argument of max_by must be less than or equal to 10000; found 10001");
        }
    }
}
