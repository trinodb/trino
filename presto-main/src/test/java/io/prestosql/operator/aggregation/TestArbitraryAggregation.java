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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Arrays;

import static io.prestosql.block.BlockAssertions.createArrayBigintBlock;
import static io.prestosql.block.BlockAssertions.createBooleansBlock;
import static io.prestosql.block.BlockAssertions.createDoublesBlock;
import static io.prestosql.block.BlockAssertions.createIntsBlock;
import static io.prestosql.block.BlockAssertions.createLongsBlock;
import static io.prestosql.block.BlockAssertions.createStringsBlock;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static org.testng.Assert.assertNotNull;

public class TestArbitraryAggregation
{
    private static final Metadata metadata = createTestMetadataManager();

    @Test
    public void testAllRegistered()
    {
        for (Type valueType : metadata.getTypes()) {
            assertNotNull(metadata.getAggregateFunctionImplementation(metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(valueType))));
        }
    }

    @Test
    public void testNullBoolean()
    {
        InternalAggregationFunction booleanAgg = metadata.getAggregateFunctionImplementation(metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(BOOLEAN)));
        assertAggregation(
                booleanAgg,
                null,
                createBooleansBlock((Boolean) null));
    }

    @Test
    public void testValidBoolean()
    {
        InternalAggregationFunction booleanAgg = metadata.getAggregateFunctionImplementation(
                metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(BOOLEAN)));
        assertAggregation(
                booleanAgg,
                true,
                createBooleansBlock(true, true));
    }

    @Test
    public void testNullLong()
    {
        InternalAggregationFunction longAgg = metadata.getAggregateFunctionImplementation(
                metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(BIGINT)));
        assertAggregation(
                longAgg,
                null,
                createLongsBlock(null, null));
    }

    @Test
    public void testValidLong()
    {
        InternalAggregationFunction longAgg = metadata.getAggregateFunctionImplementation(
                metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(BIGINT)));
        assertAggregation(
                longAgg,
                1L,
                createLongsBlock(1L, null));
    }

    @Test
    public void testNullDouble()
    {
        InternalAggregationFunction doubleAgg = metadata.getAggregateFunctionImplementation(
                metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(DOUBLE)));
        assertAggregation(
                doubleAgg,
                null,
                createDoublesBlock(null, null));
    }

    @Test
    public void testValidDouble()
    {
        InternalAggregationFunction doubleAgg = metadata.getAggregateFunctionImplementation(
                metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(DOUBLE)));
        assertAggregation(
                doubleAgg,
                2.0,
                createDoublesBlock(null, 2.0));
    }

    @Test
    public void testNullString()
    {
        InternalAggregationFunction stringAgg = metadata.getAggregateFunctionImplementation(
                metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(VARCHAR)));
        assertAggregation(
                stringAgg,
                null,
                createStringsBlock(null, null));
    }

    @Test
    public void testValidString()
    {
        InternalAggregationFunction stringAgg = metadata.getAggregateFunctionImplementation(
                metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(VARCHAR)));
        assertAggregation(
                stringAgg,
                "a",
                createStringsBlock("a", "a"));
    }

    @Test
    public void testNullArray()
    {
        InternalAggregationFunction arrayAgg = metadata.getAggregateFunctionImplementation(
                metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(new ArrayType(BIGINT))));
        assertAggregation(
                arrayAgg,
                null,
                createArrayBigintBlock(Arrays.asList(null, null, null, null)));
    }

    @Test
    public void testValidArray()
    {
        InternalAggregationFunction arrayAgg = metadata.getAggregateFunctionImplementation(
                metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(new ArrayType(BIGINT))));
        assertAggregation(
                arrayAgg,
                ImmutableList.of(23L, 45L),
                createArrayBigintBlock(ImmutableList.of(ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L), ImmutableList.of(23L, 45L))));
    }

    @Test
    public void testValidInt()
    {
        InternalAggregationFunction arrayAgg = metadata.getAggregateFunctionImplementation(
                metadata.resolveFunction(QualifiedName.of("arbitrary"), fromTypes(INTEGER)));
        assertAggregation(
                arrayAgg,
                3,
                createIntsBlock(3, 3, null));
    }
}
