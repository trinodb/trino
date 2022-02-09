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
package io.trino.operator.aggregation.minmaxby;

import com.google.common.collect.ImmutableList;
import io.trino.FeaturesConfig;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.metadata.TypeRegistry;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.block.BlockAssertions.createArrayBigintBlock;
import static io.trino.block.BlockAssertions.createBlockOfReals;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.createLongDecimalsBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createShortDecimalsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SqlDecimal.decimal;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.type.UnknownType.UNKNOWN;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertNotNull;

public class TestMinMaxByAggregation
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final Collection<Type> STANDARD_TYPES = new TypeRegistry(new TypeOperators(), new FeaturesConfig()).getTypes();

    @Test
    public void testAllRegistered()
    {
        Set<Type> orderableTypes = getTypes().stream()
                .filter(Type::isOrderable)
                .collect(toImmutableSet());

        for (Type keyType : orderableTypes) {
            for (Type valueType : getTypes()) {
                assertNotNull(FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("min_by"), fromTypes(valueType, keyType)));
                assertNotNull(FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("max_by"), fromTypes(valueType, keyType)));
            }
        }
    }

    private static List<Type> getTypes()
    {
        return new ImmutableList.Builder<Type>()
                .addAll(STANDARD_TYPES)
                .add(VARCHAR)
                .add(createDecimalType(1))
                .add(RowType.anonymous(ImmutableList.of(BIGINT, VARCHAR, DOUBLE)))
                .build();
    }

    @Test
    public void testMinUnknown()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(UNKNOWN, DOUBLE);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                null,
                createBooleansBlock(null, null),
                createDoublesBlock(1.0, 2.0));
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                null,
                createDoublesBlock(1.0, 2.0),
                createBooleansBlock(null, null));
    }

    @Test
    public void testMaxUnknown()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(UNKNOWN, DOUBLE);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                null,
                createBooleansBlock(null, null),
                createDoublesBlock(1.0, 2.0));
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                null,
                createDoublesBlock(1.0, 2.0),
                createBooleansBlock(null, null));
    }

    @Test
    public void testMinNull()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(DOUBLE, DOUBLE);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                1.0,
                createDoublesBlock(1.0, null),
                createDoublesBlock(1.0, 2.0));
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                10.0,
                createDoublesBlock(10.0, 9.0, 8.0, 11.0),
                createDoublesBlock(1.0, null, 2.0, null));
    }

    @Test
    public void testMaxNull()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(DOUBLE, DOUBLE);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                null,
                createDoublesBlock(1.0, null),
                createDoublesBlock(1.0, 2.0));
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                10.0,
                createDoublesBlock(8.0, 9.0, 10.0, 11.0),
                createDoublesBlock(-2.0, null, -1.0, null));
    }

    @Test
    public void testMinDoubleDouble()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(DOUBLE, DOUBLE);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                null,
                createDoublesBlock(null, null),
                createDoublesBlock(null, null));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                3.0,
                createDoublesBlock(3.0, 2.0, 5.0, 3.0),
                createDoublesBlock(1.0, 1.5, 2.0, 4.0));
    }

    @Test
    public void testMaxDoubleDouble()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(DOUBLE, DOUBLE);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                null,
                createDoublesBlock(null, null),
                createDoublesBlock(null, null));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                2.0,
                createDoublesBlock(3.0, 2.0, null),
                createDoublesBlock(1.0, 1.5, null));
    }

    @Test
    public void testMinVarcharDouble()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(DOUBLE, VARCHAR);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                100.0,
                createDoublesBlock(100.0, 1.0, 50.0, 2.0),
                createStringsBlock("a", "b", "c", "d"));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                -1.0,
                createDoublesBlock(100.0, 50.0, 2.0, -1.0),
                createStringsBlock("x", "y", "z", "a"));
    }

    @Test
    public void testMinDoubleVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, DOUBLE);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                "z",
                createStringsBlock("z", "a", "x", "b"),
                createDoublesBlock(1.0, 2.0, 2.0, 3.0));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                "a",
                createStringsBlock("zz", "hi", "bb", "a"),
                createDoublesBlock(0.0, 1.0, 2.0, -1.0));
    }

    @Test
    public void testMaxDoubleVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, DOUBLE);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                "a",
                createStringsBlock("z", "a", null),
                createDoublesBlock(1.0, 2.0, null));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                "hi",
                createStringsBlock("zz", "hi", null, "a"),
                createDoublesBlock(0.0, 1.0, null, -1.0));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                "c",
                createStringsBlock("a", "b", "c"),
                createDoublesBlock(Double.NaN, 1.0, 2.0));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                "c",
                createStringsBlock("a", "b", "c"),
                createDoublesBlock(1.0, Double.NaN, 2.0));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                "b",
                createStringsBlock("a", "b", "c"),
                createDoublesBlock(1.0, 2.0, Double.NaN));
    }

    @Test
    public void testMinRealVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, REAL);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                "z",
                createStringsBlock("z", "a", "x", "b"),
                createBlockOfReals(1.0f, 2.0f, 2.0f, 3.0f));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                "a",
                createStringsBlock("zz", "hi", "bb", "a"),
                createBlockOfReals(0.0f, 1.0f, 2.0f, -1.0f));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                "b",
                createStringsBlock("a", "b", "c"),
                createBlockOfReals(Float.NaN, 1.0f, 2.0f));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                "a",
                createStringsBlock("a", "b", "c"),
                createBlockOfReals(1.0f, Float.NaN, 2.0f));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                "a",
                createStringsBlock("a", "b", "c"),
                createBlockOfReals(1.0f, 2.0f, Float.NaN));
    }

    @Test
    public void testMaxRealVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, REAL);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                "a",
                createStringsBlock("z", "a", null),
                createBlockOfReals(1.0f, 2.0f, null));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                "hi",
                createStringsBlock("zz", "hi", null, "a"),
                createBlockOfReals(0.0f, 1.0f, null, -1.0f));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                "c",
                createStringsBlock("a", "b", "c"),
                createBlockOfReals(Float.NaN, 1.0f, 2.0f));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                "c",
                createStringsBlock("a", "b", "c"),
                createBlockOfReals(1.0f, Float.NaN, 2.0f));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                "b",
                createStringsBlock("a", "b", "c"),
                createBlockOfReals(1.0f, 2.0f, Float.NaN));
    }

    @Test
    public void testMinLongLongArray()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(new ArrayType(BIGINT), BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of(8L, 9L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(1L, 2L), ImmutableList.of(6L, 7L), ImmutableList.of(2L, 3L))),
                createLongsBlock(1L, 2L, 2L, 3L));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                ImmutableList.of(2L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(6L, 7L), ImmutableList.of(2L, 3L), ImmutableList.of(2L))),
                createLongsBlock(0L, 1L, 2L, -1L));
    }

    @Test
    public void testMinLongArrayLong()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(BIGINT, new ArrayType(BIGINT));
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                3L,
                createLongsBlock(1L, 2L, 2L, 3L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(1L, 2L), ImmutableList.of(6L, 7L), ImmutableList.of(1L, 1L))));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                -1L,
                createLongsBlock(0L, 1L, 2L, -1L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(6L, 7L), ImmutableList.of(-1L, -3L), ImmutableList.of(-1L))));
    }

    @Test
    public void testMaxLongArrayLong()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(BIGINT, new ArrayType(BIGINT));
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                1L,
                createLongsBlock(1L, 2L, 2L, 3L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(8L, 9L), ImmutableList.of(1L, 2L), ImmutableList.of(6L, 7L), ImmutableList.of(1L, 1L))));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                2L,
                createLongsBlock(0L, 1L, 2L, -1L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(-8L, 9L), ImmutableList.of(-6L, 7L), ImmutableList.of(-1L, -3L), ImmutableList.of(-1L))));
    }

    @Test
    public void testMaxLongLongArray()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(new ArrayType(BIGINT), BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of(1L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), asList(1L, 2L), null)),
                createLongsBlock(1L, 2L, null));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                ImmutableList.of(2L, 3L),
                createArrayBigintBlock(asList(asList(3L, 4L), asList(2L, 3L), null, asList(1L, 2L))),
                createLongsBlock(0L, 1L, null, -1L));
    }

    @Test
    public void testMinLongDecimalDecimal()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(createDecimalType(19, 1), createDecimalType(19, 1));
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                decimal("2.2", createDecimalType(19, 1)),
                createLongDecimalsBlock("1.1", "2.2", "3.3"),
                createLongDecimalsBlock("1.2", "1.0", "2.0"));
    }

    @Test
    public void testMaxLongDecimalDecimal()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(createDecimalType(19, 1), createDecimalType(19, 1));
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                decimal("3.3", createDecimalType(19, 1)),
                createLongDecimalsBlock("1.1", "2.2", "3.3", "4.4"),
                createLongDecimalsBlock("1.2", "1.0", "2.0", "1.5"));
    }

    @Test
    public void testMinShortDecimalDecimal()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(createDecimalType(10, 1), createDecimalType(10, 1));
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                decimal("2.2", createDecimalType(10, 1)),
                createShortDecimalsBlock("1.1", "2.2", "3.3"),
                createShortDecimalsBlock("1.2", "1.0", "2.0"));
    }

    @Test
    public void testMaxShortDecimalDecimal()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(createDecimalType(10, 1), createDecimalType(10, 1));
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                decimal("3.3", createDecimalType(10, 1)),
                createShortDecimalsBlock("1.1", "2.2", "3.3", "4.4"),
                createShortDecimalsBlock("1.2", "1.0", "2.0", "1.5"));
    }

    @Test
    public void testMinBooleanVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, BOOLEAN);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                "b",
                createStringsBlock("a", "b", "c"),
                createBooleansBlock(true, false, true));
    }

    @Test
    public void testMaxBooleanVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, BOOLEAN);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                "c",
                createStringsBlock("a", "b", "c"),
                createBooleansBlock(false, false, true));
    }

    @Test
    public void testMinIntegerVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, INTEGER);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                "a",
                createStringsBlock("a", "b", "c"),
                createIntsBlock(1, 2, 3));
    }

    @Test
    public void testMaxIntegerVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, INTEGER);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                "c",
                createStringsBlock("a", "b", "c"),
                createIntsBlock(1, 2, 3));
    }

    @Test
    public void testMinBooleanLongArray()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(new ArrayType(BIGINT), BOOLEAN);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                null,
                createArrayBigintBlock(asList(asList(3L, 4L), null, null)),
                createBooleansBlock(true, false, true));
    }

    @Test
    public void testMaxBooleanLongArray()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(new ArrayType(BIGINT), BOOLEAN);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                asList(2L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createBooleansBlock(false, false, true));
    }

    @Test
    public void testMinLongVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                "a",
                createStringsBlock("a", "b", "c"),
                createLongsBlock(1, 2, 3));
    }

    @Test
    public void testMaxLongVarchar()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, BIGINT);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                "c",
                createStringsBlock("a", "b", "c"),
                createLongsBlock(1, 2, 3));
    }

    @Test
    public void testMinDoubleLongArray()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(new ArrayType(BIGINT), DOUBLE);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                asList(3L, 4L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createDoublesBlock(1.0, 2.0, 3.0));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                null,
                createArrayBigintBlock(asList(null, null, asList(2L, 2L))),
                createDoublesBlock(0.0, 1.0, 2.0));
    }

    @Test
    public void testMaxDoubleLongArray()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(new ArrayType(BIGINT), DOUBLE);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                null,
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createDoublesBlock(1.0, 2.0, null));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                asList(2L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createDoublesBlock(0.0, 1.0, 2.0));
    }

    @Test
    public void testMinSliceLongArray()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(new ArrayType(BIGINT), VARCHAR);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                asList(3L, 4L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createStringsBlock("a", "b", "c"));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                null,
                createArrayBigintBlock(asList(null, null, asList(2L, 2L))),
                createStringsBlock("a", "b", "c"));
    }

    @Test
    public void testMaxSliceLongArray()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(new ArrayType(BIGINT), VARCHAR);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                asList(2L, 2L),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))),
                createStringsBlock("a", "b", "c"));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                null,
                createArrayBigintBlock(asList(asList(3L, 4L), null, null)),
                createStringsBlock("a", "b", "c"));
    }

    @Test
    public void testMinLongArrayLongArray()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(new ArrayType(BIGINT), new ArrayType(BIGINT));
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                asList(1L, 2L),
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMaxLongArrayLongArray()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(new ArrayType(BIGINT), new ArrayType(BIGINT));
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                asList(3L, 3L),
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMinLongArraySlice()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, new ArrayType(BIGINT));
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                "c",
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMaxLongArraySlice()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, new ArrayType(BIGINT));
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                "a",
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(asList(3L, 4L), null, asList(2L, 2L))));
    }

    @Test
    public void testMinUnknownSlice()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, UNKNOWN);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                null,
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(null, null, null)));
    }

    @Test
    public void testMaxUnknownSlice()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(VARCHAR, UNKNOWN);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                null,
                createStringsBlock("a", "b", "c"),
                createArrayBigintBlock(asList(null, null, null)));
    }

    @Test
    public void testMinUnknownLongArray()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(new ArrayType(BIGINT), UNKNOWN);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("min_by"),
                parameterTypes,
                null,
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(null, null, null)));
    }

    @Test
    public void testMaxUnknownLongArray()
    {
        List<TypeSignatureProvider> parameterTypes = fromTypes(new ArrayType(BIGINT), UNKNOWN);
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("max_by"),
                parameterTypes,
                null,
                createArrayBigintBlock(asList(asList(3L, 3L), null, asList(1L, 2L))),
                createArrayBigintBlock(asList(null, null, null)));
    }
}
