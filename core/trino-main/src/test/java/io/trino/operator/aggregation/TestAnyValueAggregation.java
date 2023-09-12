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
import io.trino.FeaturesConfig;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.metadata.TypeRegistry;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.sql.tree.QualifiedName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;

import static io.trino.block.BlockAssertions.createArrayBigintBlock;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createIntsBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertNotNull;

public class TestAnyValueAggregation
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    @Test
    public void testAllRegistered()
    {
        Collection<Type> standardTypes = new TypeRegistry(new TypeOperators(), new FeaturesConfig()).getTypes();
        for (Type valueType : standardTypes) {
            assertNotNull(FUNCTION_RESOLUTION.getAggregateFunction(QualifiedName.of("any_value"), fromTypes(valueType)));
        }
    }

    @Test
    public void testNullBoolean()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("any_value"),
                fromTypes(BOOLEAN),
                null,
                createBooleansBlock((Boolean) null));
    }

    @Test
    public void testValidBoolean()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("any_value"),
                fromTypes(BOOLEAN),
                true,
                createBooleansBlock(true, true));
    }

    @Test
    public void testNullLong()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("any_value"),
                fromTypes(BIGINT),
                null,
                createLongsBlock(null, null));
    }

    @Test
    public void testValidLong()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("any_value"),
                fromTypes(BIGINT),
                1L,
                createLongsBlock(1L, null));
    }

    @Test
    public void testNullDouble()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("any_value"),
                fromTypes(DOUBLE),
                null,
                createDoublesBlock(null, null));
    }

    @Test
    public void testValidDouble()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("any_value"),
                fromTypes(DOUBLE),
                2.0,
                createDoublesBlock(null, 2.0));
    }

    @Test
    public void testNullString()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("any_value"),
                fromTypes(VARCHAR),
                null,
                createStringsBlock(null, null));
    }

    @Test
    public void testValidString()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("any_value"),
                fromTypes(VARCHAR),
                "a",
                createStringsBlock("a", "a"));
    }

    @Test
    public void testNullArray()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("any_value"),
                fromTypes(new ArrayType(BIGINT)),
                null,
                createArrayBigintBlock(Arrays.asList(null, null, null, null)));
    }

    @Test
    public void testValidArray()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("any_value"),
                fromTypes(new ArrayType(BIGINT)),
                ImmutableList.of(23L, 45L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L))));
    }

    @Test
    public void testValidInt()
    {
        assertAggregation(
                FUNCTION_RESOLUTION,
                QualifiedName.of("any_value"),
                fromTypes(INTEGER),
                3,
                createIntsBlock(3, 3, null));
    }
}
