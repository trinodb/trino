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
package io.trino.type;

import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.collect.ImmutableList;
import io.trino.json.ir.IrAbsMethod;
import io.trino.json.ir.IrArithmeticBinary;
import io.trino.json.ir.IrArithmeticUnary;
import io.trino.json.ir.IrArrayAccessor;
import io.trino.json.ir.IrArrayAccessor.Subscript;
import io.trino.json.ir.IrCeilingMethod;
import io.trino.json.ir.IrConstantJsonSequence;
import io.trino.json.ir.IrContextVariable;
import io.trino.json.ir.IrDatetimeMethod;
import io.trino.json.ir.IrDescendantMemberAccessor;
import io.trino.json.ir.IrDoubleMethod;
import io.trino.json.ir.IrFloorMethod;
import io.trino.json.ir.IrJsonPath;
import io.trino.json.ir.IrKeyValueMethod;
import io.trino.json.ir.IrLastIndexVariable;
import io.trino.json.ir.IrMemberAccessor;
import io.trino.json.ir.IrNamedJsonVariable;
import io.trino.json.ir.IrNamedValueVariable;
import io.trino.json.ir.IrSizeMethod;
import io.trino.json.ir.IrTypeMethod;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.type.Type;
import org.assertj.core.api.AssertProvider;
import org.assertj.core.api.RecursiveComparisonAssert;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.json.ir.IrArithmeticBinary.Operator.ADD;
import static io.trino.json.ir.IrArithmeticBinary.Operator.MULTIPLY;
import static io.trino.json.ir.IrArithmeticUnary.Sign.MINUS;
import static io.trino.json.ir.IrArithmeticUnary.Sign.PLUS;
import static io.trino.json.ir.IrConstantJsonSequence.EMPTY_SEQUENCE;
import static io.trino.json.ir.IrConstantJsonSequence.singletonSequence;
import static io.trino.json.ir.IrJsonNull.JSON_NULL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeType.DEFAULT_PRECISION;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.planner.PathNodes.literal;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJsonPath2016TypeSerialization
{
    private static final Type JSON_PATH_2016 = new JsonPath2016Type(new TypeDeserializer(TESTING_TYPE_MANAGER), new TestingBlockEncodingSerde());
    private static final RecursiveComparisonConfiguration COMPARISON_CONFIGURATION = RecursiveComparisonConfiguration.builder().withStrictTypeChecking(true).build();

    @Test
    public void testJsonPathMode()
    {
        assertJsonRoundTrip(new IrJsonPath(true, JSON_NULL));
        assertJsonRoundTrip(new IrJsonPath(false, JSON_NULL));
    }

    @Test
    public void testLiterals()
    {
        assertJsonRoundTrip(new IrJsonPath(true, literal(createDecimalType(2, 1), 1L)));
        assertJsonRoundTrip(new IrJsonPath(true, literal(DOUBLE, 1e0)));
        assertJsonRoundTrip(new IrJsonPath(true, literal(INTEGER, 1L)));
        assertJsonRoundTrip(new IrJsonPath(true, literal(BIGINT, 1000000000000L)));
        assertJsonRoundTrip(new IrJsonPath(true, literal(VARCHAR, utf8Slice("some_text"))));
        assertJsonRoundTrip(new IrJsonPath(true, literal(BOOLEAN, false)));
    }

    @Test
    public void testContextVariable()
    {
        assertJsonRoundTrip(new IrJsonPath(true, new IrContextVariable(Optional.empty())));
        assertJsonRoundTrip(new IrJsonPath(true, new IrContextVariable(Optional.of(DOUBLE))));
    }

    @Test
    public void testNamedVariables()
    {
        // json variable
        assertJsonRoundTrip(new IrJsonPath(true, new IrNamedJsonVariable(5, Optional.empty())));
        assertJsonRoundTrip(new IrJsonPath(true, new IrNamedJsonVariable(5, Optional.of(DOUBLE))));

        // SQL value variable
        assertJsonRoundTrip(new IrJsonPath(true, new IrNamedValueVariable(5, Optional.of(DOUBLE))));
    }

    @Test
    public void testMethods()
    {
        assertJsonRoundTrip(new IrJsonPath(true, new IrAbsMethod(literal(DOUBLE, 1e0), Optional.of(DOUBLE))));
        assertJsonRoundTrip(new IrJsonPath(true, new IrCeilingMethod(literal(DOUBLE, 1e0), Optional.of(DOUBLE))));
        assertJsonRoundTrip(new IrJsonPath(true, new IrDatetimeMethod(literal(BIGINT, 1L), Optional.of("some_time_format"), Optional.of(createTimeType(DEFAULT_PRECISION)))));
        assertJsonRoundTrip(new IrJsonPath(true, new IrDoubleMethod(literal(BIGINT, 1L), Optional.of(DOUBLE))));
        assertJsonRoundTrip(new IrJsonPath(true, new IrFloorMethod(literal(DOUBLE, 1e0), Optional.of(DOUBLE))));
        assertJsonRoundTrip(new IrJsonPath(true, new IrKeyValueMethod(JSON_NULL)));
        assertJsonRoundTrip(new IrJsonPath(true, new IrSizeMethod(JSON_NULL, Optional.of(INTEGER))));
        assertJsonRoundTrip(new IrJsonPath(true, new IrTypeMethod(JSON_NULL, Optional.of(createVarcharType(7)))));
    }

    @Test
    public void testArrayAccessor()
    {
        // wildcard accessor
        assertJsonRoundTrip(new IrJsonPath(true, new IrArrayAccessor(JSON_NULL, ImmutableList.of(), Optional.empty())));

        // with subscripts based on literals
        assertJsonRoundTrip(new IrJsonPath(true, new IrArrayAccessor(
                JSON_NULL,
                ImmutableList.of(
                        new Subscript(literal(INTEGER, 0L), Optional.of(literal(INTEGER, 1L))),
                        new Subscript(literal(INTEGER, 3L), Optional.of(literal(INTEGER, 5L))),
                        new Subscript(literal(INTEGER, 7L), Optional.empty())),
                Optional.of(VARCHAR))));

        // with LAST index variable
        assertJsonRoundTrip(new IrJsonPath(true, new IrArrayAccessor(
                JSON_NULL,
                ImmutableList.of(new Subscript(new IrLastIndexVariable(Optional.of(INTEGER)), Optional.empty())),
                Optional.empty())));
    }

    @Test
    public void testMemberAccessor()
    {
        // wildcard accessor
        assertJsonRoundTrip(new IrJsonPath(true, new IrMemberAccessor(JSON_NULL, Optional.empty(), Optional.empty())));

        // accessor by field name
        assertJsonRoundTrip(new IrJsonPath(true, new IrMemberAccessor(JSON_NULL, Optional.of("some_key"), Optional.of(BIGINT))));
    }

    @Test
    public void testDescendantMemberAccessor()
    {
        assertJsonRoundTrip(new IrJsonPath(true, new IrDescendantMemberAccessor(JSON_NULL, "some_key", Optional.empty())));
    }

    @Test
    public void testArithmeticBinary()
    {
        assertJsonRoundTrip(new IrJsonPath(true, new IrArithmeticBinary(ADD, JSON_NULL, JSON_NULL, Optional.empty())));

        assertJsonRoundTrip(new IrJsonPath(true, new IrArithmeticBinary(
                ADD,
                literal(INTEGER, 1L),
                literal(BIGINT, 2L),
                Optional.of(BIGINT))));
    }

    @Test
    public void testArithmeticUnary()
    {
        assertJsonRoundTrip(new IrJsonPath(true, new IrArithmeticUnary(PLUS, JSON_NULL, Optional.empty())));
        assertJsonRoundTrip(new IrJsonPath(true, new IrArithmeticUnary(MINUS, JSON_NULL, Optional.empty())));
        assertJsonRoundTrip(new IrJsonPath(true, new IrArithmeticUnary(MINUS, literal(INTEGER, 1L), Optional.of(INTEGER))));
    }

    @Test
    public void testConstantJsonSequence()
    {
        // empty sequence
        assertJsonRoundTrip(new IrJsonPath(true, EMPTY_SEQUENCE));

        // singleton sequence
        assertJsonRoundTrip(new IrJsonPath(true, singletonSequence(NullNode.getInstance(), Optional.empty())));
        assertJsonRoundTrip(new IrJsonPath(true, singletonSequence(BooleanNode.TRUE, Optional.of(BOOLEAN))));

        // long sequence
        assertJsonRoundTrip(new IrJsonPath(true, new IrConstantJsonSequence(
                ImmutableList.of(IntNode.valueOf(1), IntNode.valueOf(2), IntNode.valueOf(3)),
                Optional.of(INTEGER))));
    }

    @Test
    public void testNestedStructure()
    {
        assertJsonRoundTrip(new IrJsonPath(
                true,
                new IrTypeMethod(
                        new IrArithmeticBinary(
                                MULTIPLY,
                                new IrArithmeticUnary(MINUS, new IrAbsMethod(new IrFloorMethod(literal(INTEGER, 1L), Optional.of(INTEGER)), Optional.of(INTEGER)), Optional.of(INTEGER)),
                                new IrCeilingMethod(new IrMemberAccessor(new IrContextVariable(Optional.empty()), Optional.of("some_key"), Optional.of(BIGINT)), Optional.of(BIGINT)),
                                Optional.of(BIGINT)),
                        Optional.of(createVarcharType(7)))));
    }

    private static void assertJsonRoundTrip(IrJsonPath object)
    {
        BlockBuilder blockBuilder = JSON_PATH_2016.createBlockBuilder(null, 1);
        JSON_PATH_2016.writeObject(blockBuilder, object);
        Block serialized = blockBuilder.build();
        Object deserialized = JSON_PATH_2016.getObject(serialized, 0);
        assertThat((AssertProvider<? extends RecursiveComparisonAssert<?>>) () -> new RecursiveComparisonAssert<>(deserialized, COMPARISON_CONFIGURATION))
                .isEqualTo(object);
    }
}
