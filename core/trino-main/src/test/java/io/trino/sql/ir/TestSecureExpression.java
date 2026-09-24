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
package io.trino.sql.ir;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.block.BlockJsonSerde;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeDescriptor;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolKeyDeserializer;
import io.trino.type.TypeDescriptorKeyDeserializer;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ir.Booleans.TRUE;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.IrUtils.extractConjuncts;
import static io.trino.sql.ir.TestingIr.comparison;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TestingSymbolAllocator.emptySymbolAllocator;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

public class TestSecureExpression
{
    private static final JsonCodec<Expression> EXPRESSION_CODEC;

    static {
        JsonMapper objectMapper = new JsonMapperProvider()
                .withKeyDeserializers(ImmutableMap.of(
                        TypeDescriptor.class, new TypeDescriptorKeyDeserializer(),
                        Symbol.class, new SymbolKeyDeserializer(TESTING_TYPE_MANAGER)))
                .withJsonDeserializers(ImmutableMap.of(
                        Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER),
                        Block.class, new BlockJsonSerde.Deserializer(TESTING_BLOCK_ENCODING_SERDE)))
                .withJsonSerializers(ImmutableMap.of(
                        Block.class, new BlockJsonSerde.Serializer(TESTING_BLOCK_ENCODING_SERDE)))
                .get();
        EXPRESSION_CODEC = new JsonCodecFactory(objectMapper).jsonCodec(Expression.class);
    }

    @Test
    public void testTypeAndRendering()
    {
        RowType rowType = RowType.anonymous(ImmutableList.of(BIGINT, VARCHAR));
        for (Type type : ImmutableList.of(BOOLEAN, BIGINT, DOUBLE, VARCHAR, rowType)) {
            SecureExpression secure = new SecureExpression(new Reference(type, "policy_secret"));
            assertThat(secure.type()).isEqualTo(type);
            assertThat(secure.toString()).isEqualTo(SecureExpression.REDACTED);
        }
    }

    @Test
    public void testEvaluationIsTransparent()
    {
        assertThat(evaluate(new Constant(BOOLEAN, true))).isEqualTo(true);
        assertThat(evaluate(new Constant(BIGINT, 42L))).isEqualTo(42L);
        assertThat(evaluate(new Constant(DOUBLE, 3.5))).isEqualTo(3.5);
        assertThat(evaluate(new Constant(VARCHAR, utf8Slice("value")))).isEqualTo(utf8Slice("value"));
        assertThat(evaluate(new Constant(BIGINT, null))).isNull();
    }

    @Test
    public void testEvaluationFailureIsRedacted()
    {
        TrinoException failure = catchThrowableOfType(
                TrinoException.class,
                () -> evaluate(new Cast(new Constant(VARCHAR, utf8Slice("policy-secret")), BIGINT)));

        assertThat(failure.getErrorCode()).isEqualTo(INVALID_CAST_ARGUMENT.toErrorCode());
        assertThat(failure).hasMessage(SecureExpression.REDACTED);
        assertThat(failure.getCause()).isNull();
    }

    @Test
    public void testDefaultVisitorDoesNotBypassSecureExpression()
    {
        SecureExpression secure = new SecureExpression(new Reference(BIGINT, "policy_secret"));

        assertThat(new IrVisitor<Class<?>, Void>()
        {
            @Override
            protected Class<?> visitExpression(Expression node, Void context)
            {
                return node.getClass();
            }
        }.process(secure)).isEqualTo(SecureExpression.class);
    }

    @Test
    public void testConstantFoldingKeepsMarker()
    {
        Expression foldable = comparison(EQUAL, new Constant(BIGINT, 1L), new Constant(BIGINT, 1L));
        assertThat(PLANNER_CONTEXT.getExpressionOptimizer().process(new SecureExpression(foldable), testSession(), emptySymbolAllocator(), ImmutableMap.of()))
                .isEqualTo(Optional.of(new SecureExpression(TRUE)));

        // Anything else keeps its marker
        SecureExpression secure = new SecureExpression(comparison(GREATER_THAN, new Reference(BIGINT, "policy_secret"), new Constant(BIGINT, 100000L)));
        assertThat(PLANNER_CONTEXT.getExpressionOptimizer().process(secure, testSession(), emptySymbolAllocator(), ImmutableMap.of()).orElse(secure))
                .isEqualTo(secure);
    }

    @Test
    public void testConstantMaskKeepsItsPolicyLiteralSecure()
    {
        SecureExpression secure = new SecureExpression(new Constant(BIGINT, 987654321L));
        Expression optimized = PLANNER_CONTEXT.getExpressionOptimizer().process(secure, testSession(), emptySymbolAllocator(), ImmutableMap.of()).orElse(secure);

        assertThat(optimized).isEqualTo(secure);
        assertThat(optimized.toString()).doesNotContain("987654321");
    }

    @Test
    public void testConjunctExtractionKeepsSecureExpressionOpaque()
    {
        Expression comparison = comparison(GREATER_THAN, new Reference(BIGINT, "policy_secret"), new Constant(BIGINT, 100000L));
        SecureExpression secure = new SecureExpression(comparison);

        assertThat(extractConjuncts(secure)).containsExactly(secure);
    }

    @Test
    public void testJsonRoundTripForWorkerPlan()
    {
        Expression secure = new SecureExpression(comparison(
                GREATER_THAN,
                new Reference(BIGINT, "policy_secret"),
                new Constant(BIGINT, 100000L)));

        assertThat(EXPRESSION_CODEC.fromJson(EXPRESSION_CODEC.toJson(secure))).isEqualTo(secure);
    }

    private static Object evaluate(Expression expression)
    {
        return PLANNER_CONTEXT.getExpressionEvaluator().evaluate(new SecureExpression(expression), testSession(), ImmutableMap.of());
    }
}
