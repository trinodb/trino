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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.ArrayType;
import io.trino.sql.analyzer.TypeSignatureProvider;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.block.BlockAssertions.createBlockOfReals;
import static io.trino.block.BlockAssertions.createDoubleRepeatBlock;
import static io.trino.block.BlockAssertions.createDoubleSequenceBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createSequenceBlockOfReal;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;

public class TestApproximatePercentileAggregation
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    private static final List<TypeSignatureProvider> DOUBLE_APPROXIMATE_PERCENTILE = fromTypes(DOUBLE, DOUBLE);
    private static final List<TypeSignatureProvider> DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED = fromTypes(DOUBLE, BIGINT, DOUBLE);
    private static final List<TypeSignatureProvider> DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_WITH_ACCURACY = fromTypes(DOUBLE, BIGINT, DOUBLE, DOUBLE);

    private static final List<TypeSignatureProvider> LONG_APPROXIMATE_PERCENTILE = fromTypes(BIGINT, DOUBLE);
    private static final List<TypeSignatureProvider> LONG_APPROXIMATE_PERCENTILE_WEIGHTED = fromTypes(BIGINT, BIGINT, DOUBLE);
    private static final List<TypeSignatureProvider> LONG_APPROXIMATE_PERCENTILE_WEIGHTED_WITH_ACCURACY = fromTypes(BIGINT, BIGINT, DOUBLE, DOUBLE);

    private static final List<TypeSignatureProvider> DOUBLE_APPROXIMATE_PERCENTILE_ARRAY = fromTypes(DOUBLE, new ArrayType(DOUBLE));
    private static final List<TypeSignatureProvider> DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED = fromTypes(DOUBLE, BIGINT, new ArrayType(DOUBLE));

    private static final List<TypeSignatureProvider> LONG_APPROXIMATE_PERCENTILE_ARRAY = fromTypes(BIGINT, new ArrayType(DOUBLE));
    private static final List<TypeSignatureProvider> LONG_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED = fromTypes(BIGINT, BIGINT, new ArrayType(DOUBLE));

    private static final List<TypeSignatureProvider> FLOAT_APPROXIMATE_PERCENTILE = fromTypes(REAL, DOUBLE);
    private static final List<TypeSignatureProvider> FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED = fromTypes(REAL, BIGINT, DOUBLE);
    private static final List<TypeSignatureProvider> FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED_WITH_ACCURACY = fromTypes(REAL, BIGINT, DOUBLE, DOUBLE);

    private static final List<TypeSignatureProvider> FLOAT_APPROXIMATE_PERCENTILE_ARRAY = fromTypes(REAL, new ArrayType(DOUBLE));
    private static final List<TypeSignatureProvider> FLOAT_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED = fromTypes(REAL, BIGINT, new ArrayType(DOUBLE));

    @Test
    public void testLongPartialStep()
    {
        // regular approx_percentile
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE,
                null,
                createLongsBlock(null, null),
                createRleBlock(0.5, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE,
                1L,
                createLongsBlock(null, 1L),
                createRleBlock(0.5, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE,
                2L,
                createLongsBlock(null, 1L, 2L, 3L),
                createRleBlock(0.5, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE,
                2L,
                createLongsBlock(1L, 2L, 3L),
                createRleBlock(0.5, 3));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE,
                3L,
                createLongsBlock(1L, null, 2L, 2L, null, 2L, 2L, null, 2L, 2L, null, 3L, 3L, null, 3L, null, 3L, 4L, 5L, 6L, 7L),
                createRleBlock(0.5, 21));

        // array of approx_percentile
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_ARRAY,
                null,
                createLongsBlock(null, null),
                createRleBlock(ImmutableList.of(0.5), 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_ARRAY,
                null,
                createLongsBlock(null, null),
                createRleBlock(ImmutableList.of(0.5, 0.99), 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_ARRAY,
                ImmutableList.of(1L, 1L),
                createLongsBlock(null, 1L),
                createRleBlock(ImmutableList.of(0.5, 0.5), 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_ARRAY,
                ImmutableList.of(1L, 2L, 3L),
                createLongsBlock(null, 1L, 2L, 3L),
                createRleBlock(ImmutableList.of(0.2, 0.5, 0.8), 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_ARRAY,
                ImmutableList.of(2L, 3L),
                createLongsBlock(1L, 2L, 3L),
                createRleBlock(ImmutableList.of(0.5, 0.99), 3));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_ARRAY,
                ImmutableList.of(1L, 3L),
                createLongsBlock(1L, null, 2L, 2L, null, 2L, 2L, null, 2L, 2L, null, 3L, 3L, null, 3L, null, 3L, 4L, 5L, 6L, 7L),
                createRleBlock(ImmutableList.of(0.01, 0.5), 21));

        // unsorted percentiles
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_ARRAY,
                ImmutableList.of(3L, 1L, 2L),
                createLongsBlock(null, 1L, 2L, 3L),
                createRleBlock(ImmutableList.of(0.8, 0.2, 0.5), 4));

        // weighted approx_percentile
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED,
                null,
                createLongsBlock(null, null),
                createLongsBlock(1L, 1L),
                createRleBlock(0.5, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED,
                1L,
                createLongsBlock(null, 1L),
                createDoublesBlock(1.0, 1.0),
                createRleBlock(0.5, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED,
                2L,
                createLongsBlock(null, 1L, 2L, 3L),
                createDoublesBlock(1.0, 1.0, 1.0, 1.0),
                createRleBlock(0.5, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED,
                2L,
                createLongsBlock(1L, 2L, 3L),
                createDoublesBlock(1.0, 1.0, 1.0),
                createRleBlock(0.5, 3));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED,
                2L,
                createLongsBlock(1L, 2L, 3L),
                createDoublesBlock(23.4, 23.4, 23.4),
                createRleBlock(0.5, 3));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED,
                3L,
                createLongsBlock(1L, null, 2L, null, 2L, null, 2L, null, 3L, null, 3L, null, 3L, 4L, 5L, 6L, 7L),
                createDoublesBlock(1.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
                createRleBlock(0.5, 17));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED,
                3L,
                createLongsBlock(1L, null, 2L, null, 2L, null, 2L, null, 3L, null, 3L, null, 3L, 4L, 5L, 6L, 7L),
                createDoublesBlock(1.1, 1.1, 2.2, 1.1, 2.2, 1.1, 2.2, 1.1, 2.2, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1),
                createRleBlock(0.5, 17));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_WEIGHTED_WITH_ACCURACY,
                9900L,
                createLongSequenceBlock(0, 10000),
                createDoubleRepeatBlock(1.0, 10000),
                createRleBlock(0.99, 10000),
                createRleBlock(0.001, 10000));

        // weighted + array of approx_percentile
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                LONG_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED,
                ImmutableList.of(2L, 3L),
                createLongsBlock(1L, 2L, 3L),
                createDoublesBlock(4.0, 2.0, 1.0),
                createRleBlock(ImmutableList.of(0.5, 0.8), 3));
    }

    @Test
    public void testFloatPartialStep()
    {
        // regular approx_percentile
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE,
                null,
                createBlockOfReals(null, null),
                createRleBlock(0.5, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE,
                1.0f,
                createBlockOfReals(null, 1.0f),
                createRleBlock(0.5, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE,
                2.0f,
                createBlockOfReals(null, 1.0f, 2.0f, 3.0f),
                createRleBlock(0.5, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE,
                1.0f,
                createBlockOfReals(-1.0f, 1.0f),
                createRleBlock(0.5, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE,
                -1.0f,
                createBlockOfReals(-2.0f, 3.0f, -1.0f),
                createRleBlock(0.5, 3));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE,
                2.0f,
                createBlockOfReals(1.0f, 2.0f, 3.0f),
                createRleBlock(0.5, 3));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE,
                3.0f,
                createBlockOfReals(1.0f, null, 2.0f, 2.0f, null, 2.0f, 2.0f, null, 2.0f, 2.0f, null, 3.0f, 3.0f, null, 3.0f, null, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f),
                createRleBlock(0.5, 21));

        // array of approx_percentile
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE_ARRAY,
                null,
                createBlockOfReals(null, null),
                createRleBlock(ImmutableList.of(0.5), 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE_ARRAY,
                null,
                createBlockOfReals(null, null),
                createRleBlock(ImmutableList.of(0.5, 0.5), 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE_ARRAY,
                ImmutableList.of(1.0f, 1.0f),
                createBlockOfReals(null, 1.0f),
                createRleBlock(ImmutableList.of(0.5, 0.5), 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE_ARRAY,
                ImmutableList.of(1.0f, 2.0f, 3.0f),
                createBlockOfReals(null, 1.0f, 2.0f, 3.0f),
                createRleBlock(ImmutableList.of(0.2, 0.5, 0.8), 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE_ARRAY,
                ImmutableList.of(2.0f, 3.0f),
                createBlockOfReals(1.0f, 2.0f, 3.0f),
                createRleBlock(ImmutableList.of(0.5, 0.99), 3));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE_ARRAY,
                ImmutableList.of(1.0f, 3.0f),
                createBlockOfReals(1.0f, null, 2.0f, 2.0f, null, 2.0f, 2.0f, null, 2.0f, 2.0f, null, 3.0f, 3.0f, null, 3.0f, null, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f),
                createRleBlock(ImmutableList.of(0.01, 0.5), 21));

        // unsorted percentiles
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE_ARRAY,
                ImmutableList.of(3.0f, 1.0f, 2.0f),
                createBlockOfReals(null, 1.0f, 2.0f, 3.0f),
                createRleBlock(ImmutableList.of(0.8, 0.2, 0.5), 4));

        // weighted approx_percentile
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED,
                null,
                createBlockOfReals(null, null),
                createLongsBlock(1L, 1L),
                createRleBlock(0.5, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED,
                1.0f,
                createBlockOfReals(null, 1.0f),
                createDoublesBlock(1.0, 1.0),
                createRleBlock(0.5, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED,
                2.0f,
                createBlockOfReals(null, 1.0f, 2.0f, 3.0f),
                createDoublesBlock(1.0, 1.0, 1.0, 1.0),
                createRleBlock(0.5, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED,
                2.0f,
                createBlockOfReals(1.0f, 2.0f, 3.0f),
                createDoublesBlock(1.0, 1.0, 1.0),
                createRleBlock(0.5, 3));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED,
                2.75f,
                createBlockOfReals(1.0f, null, 2.0f, null, 2.0f, null, 2.0f, null, 3.0f, null, 3.0f, null, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f),
                createDoublesBlock(1.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
                createRleBlock(0.5, 17));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED,
                2.75f,
                createBlockOfReals(1.0f, null, 2.0f, null, 2.0f, null, 2.0f, null, 3.0f, null, 3.0f, null, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f),
                createDoublesBlock(1.1, 1.1, 2.2, 1.1, 2.2, 1.1, 2.2, 1.1, 2.2, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1),
                createRleBlock(0.5, 17));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE_WEIGHTED_WITH_ACCURACY,
                9900.0f,
                createSequenceBlockOfReal(0, 10000),
                createDoubleRepeatBlock(1, 10000),
                createRleBlock(0.99, 10000),
                createRleBlock(0.001, 10000));

        // weighted + array of approx_percentile
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                FLOAT_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED,
                ImmutableList.of(1.5f, 2.6f),
                createBlockOfReals(1.0f, 2.0f, 3.0f),
                createDoublesBlock(4.0, 2.0, 1.0),
                createRleBlock(ImmutableList.of(0.5, 0.8), 3));
    }

    @Test
    public void testDoublePartialStep()
    {
        // regular approx_percentile
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE,
                null,
                createDoublesBlock(null, null),
                createRleBlock(0.5, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE,
                1.0,
                createDoublesBlock(null, 1.0),
                createRleBlock(0.5, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE,
                2.0,
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createRleBlock(0.5, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE,
                2.0,
                createDoublesBlock(1.0, 2.0, 3.0),
                createRleBlock(0.5, 3));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE,
                3.0,
                createDoublesBlock(1.0, null, 2.0, 2.0, null, 2.0, 2.0, null, 2.0, 2.0, null, 3.0, 3.0, null, 3.0, null, 3.0, 4.0, 5.0, 6.0, 7.0),
                createRleBlock(0.5, 21));

        // array of approx_percentile
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE_ARRAY,
                null,
                createDoublesBlock(null, null),
                createRleBlock(ImmutableList.of(0.5), 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE_ARRAY,
                null,
                createDoublesBlock(null, null),
                createRleBlock(ImmutableList.of(0.5, 0.5), 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE_ARRAY,
                ImmutableList.of(1.0, 1.0),
                createDoublesBlock(null, 1.0),
                createRleBlock(ImmutableList.of(0.5, 0.5), 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE_ARRAY,
                ImmutableList.of(1.0, 2.0, 3.0),
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createRleBlock(ImmutableList.of(0.2, 0.5, 0.8), 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE_ARRAY,
                ImmutableList.of(2.0, 3.0),
                createDoublesBlock(1.0, 2.0, 3.0),
                createRleBlock(ImmutableList.of(0.5, 0.99), 3));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE_ARRAY,
                ImmutableList.of(1.0, 3.0),
                createDoublesBlock(1.0, null, 2.0, 2.0, null, 2.0, 2.0, null, 2.0, 2.0, null, 3.0, 3.0, null, 3.0, null, 3.0, 4.0, 5.0, 6.0, 7.0),
                createRleBlock(ImmutableList.of(0.01, 0.5), 21));

        // unsorted percentiles
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE_ARRAY,
                ImmutableList.of(3.0, 1.0, 2.0),
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createRleBlock(ImmutableList.of(0.8, 0.2, 0.5), 4));

        // weighted approx_percentile
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED,
                null,
                createDoublesBlock(null, null),
                createLongsBlock(1L, 1L),
                createRleBlock(0.5, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED,
                1.0,
                createDoublesBlock(null, 1.0),
                createDoublesBlock(1.0, 1.0),
                createRleBlock(0.5, 2));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED,
                2.0,
                createDoublesBlock(null, 1.0, 2.0, 3.0),
                createDoublesBlock(1.0, 1.0, 1.0, 1.0),
                createRleBlock(0.5, 4));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED,
                2.0,
                createDoublesBlock(1.0, 2.0, 3.0),
                createDoublesBlock(1.0, 1.0, 1.0),
                createRleBlock(0.5, 3));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED,
                2.75,
                createDoublesBlock(1.0, null, 2.0, null, 2.0, null, 2.0, null, 3.0, null, 3.0, null, 3.0, 4.0, 5.0, 6.0, 7.0),
                createDoublesBlock(1.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0),
                createRleBlock(0.5, 17));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED,
                2.75,
                createDoublesBlock(1.0, null, 2.0, null, 2.0, null, 2.0, null, 3.0, null, 3.0, null, 3.0, 4.0, 5.0, 6.0, 7.0),
                createDoublesBlock(1.1, 1.1, 2.2, 1.1, 2.2, 1.1, 2.2, 1.1, 2.2, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1, 1.1),
                createRleBlock(0.5, 17));

        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_WITH_ACCURACY,
                9900.0,
                createDoubleSequenceBlock(0, 10000),
                createDoubleRepeatBlock(1.0, 10000),
                createRleBlock(0.99, 10000),
                createRleBlock(0.001, 10000));

        // weighted + array of approx_percentile
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("approx_percentile"),
                DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED,
                ImmutableList.of(1.5, 2.6000000000000005),
                createDoublesBlock(1.0, 2.0, 3.0),
                createDoublesBlock(4.0, 2.0, 1.0),
                createRleBlock(ImmutableList.of(0.5, 0.8), 3));
    }

    private static Block createRleBlock(double percentile, int positionCount)
    {
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, 1);
        DOUBLE.writeDouble(blockBuilder, percentile);
        return RunLengthEncodedBlock.create(blockBuilder.build(), positionCount);
    }

    private static Block createRleBlock(Iterable<Double> percentiles, int positionCount)
    {
        BlockBuilder rleBlockBuilder = new ArrayType(DOUBLE).createBlockBuilder(null, 1);
        BlockBuilder arrayBlockBuilder = rleBlockBuilder.beginBlockEntry();

        for (double percentile : percentiles) {
            DOUBLE.writeDouble(arrayBlockBuilder, percentile);
        }

        rleBlockBuilder.closeEntry();

        return RunLengthEncodedBlock.create(rleBlockBuilder.build(), positionCount);
    }
}
