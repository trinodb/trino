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
package io.trino.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.airlift.slice.Slice;
import io.airlift.stats.cardinality.HyperLogLog;
import io.trino.FeaturesConfig;
import io.trino.block.BlockJsonSerde;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.metadata.Metadata;
import io.trino.metadata.MetadataManager;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockEncoding;
import io.trino.spi.block.BlockEncodingSerde;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.planner.TestingPlannerContext;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.relational.ConstantExpression;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.sql.relational.optimizer.ExpressionOptimizer;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.type.TypeDeserializer;
import io.trino.type.TypeSignatureDeserializer;
import io.trino.type.TypeSignatureKeyDeserializer;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.function.OperatorType.SUBSCRIPT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ExpressionTestUtils.planExpression;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.SpecialForm.Form.ROW_CONSTRUCTOR;
import static java.lang.Float.floatToIntBits;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRowExpressionSerde
{
    private final Metadata metadata = MetadataManager.createTestMetadataManager();

    private static final PlannerContext PLANNER_CONTEXT = TestingPlannerContext.plannerContextBuilder().addFunctions(new InternalFunctionBundle()).build();

    private JsonCodec<RowExpression> codec;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        codec = getJsonCodec();
    }

    @Test
    public void testSimpleLiteral()
    {
        assertLiteral("TRUE", constant(true, BOOLEAN));
        assertLiteral("FALSE", constant(false, BOOLEAN));
        assertLiteral("CAST(NULL AS BOOLEAN)", constant(null, BOOLEAN));

        assertLiteral("TINYINT '1'", constant(1L, TINYINT));
        assertLiteral("SMALLINT '1'", constant(1L, SMALLINT));
        assertLiteral("1", constant(1L, INTEGER));
        assertLiteral("BIGINT '1'", constant(1L, BIGINT));

        assertLiteral("1.1", constant(1.1, DOUBLE));
        assertLiteral("nan()", constant(Double.NaN, DOUBLE));
        assertLiteral("infinity()", constant(Double.POSITIVE_INFINITY, DOUBLE));
        assertLiteral("-infinity()", constant(Double.NEGATIVE_INFINITY, DOUBLE));

        assertLiteral("CAST(1.1 AS REAL)", constant((long) floatToIntBits(1.1f), REAL));
        assertLiteral("CAST(nan() AS REAL)", constant((long) floatToIntBits(Float.NaN), REAL));
        assertLiteral("CAST(infinity() AS REAL)", constant((long) floatToIntBits(Float.POSITIVE_INFINITY), REAL));
        assertLiteral("CAST(-infinity() AS REAL)", constant((long) floatToIntBits(Float.NEGATIVE_INFINITY), REAL));

        assertStringLiteral("'String Literal'", "String Literal", VarcharType.createVarcharType(14));
        assertLiteral("CAST(NULL AS VARCHAR)", constant(null, VARCHAR));

        assertLiteral("DATE '1991-01-01'", constant(7670L, DATE));
        assertLiteral("TIMESTAMP '1991-01-01 00:00:00.000'", constant(662688000000000L, TIMESTAMP));
    }

    @Test
    public void testArrayLiteral()
    {
        RowExpression rowExpression = getRoundTrip("ARRAY [1, 2, 3]", true);
        assertTrue(rowExpression instanceof ConstantExpression);
        Object value = ((ConstantExpression) rowExpression).getValue();
        assertTrue(value instanceof IntArrayBlock);
        IntArrayBlock block = (IntArrayBlock) value;
        assertEquals(block.getPositionCount(), 3);
        assertEquals(block.getInt(0, 0), 1);
        assertEquals(block.getInt(1, 0), 2);
        assertEquals(block.getInt(2, 0), 3);
    }

    @Test
    public void testArrayGet()
    {
        assertEquals(getRoundTrip("(ARRAY [1, 2, 3])[1]", false),
                call(operator(SUBSCRIPT, new ArrayType(INTEGER), BIGINT),
                        call(
                                function("array_constructor", INTEGER, INTEGER, INTEGER),
                                constant(1L, INTEGER),
                                constant(2L, INTEGER),
                                constant(3L, INTEGER)),
                        call(metadata.getCoercion(TEST_SESSION, INTEGER, BIGINT), constant(1L, INTEGER))));
        assertEquals(getRoundTrip("(ARRAY [1, 2, 3])[1]", true), constant(1L, INTEGER));
    }

    @Test
    public void testRowLiteral()
    {
        assertEquals(getRoundTrip("ROW(1, 1.1)", false),
                 new SpecialForm(
                        ROW_CONSTRUCTOR,
                        RowType.anonymous(
                                ImmutableList.of(
                                        INTEGER,
                                        DOUBLE)),
                        constant(1L, INTEGER),
                        constant(1.1, DOUBLE)));
    }

    @Test
    public void testDereference()
    {
        String sql = "CAST(ROW(1) AS ROW(col1 integer)).col1";
        RowExpression before = translate(new SqlParser().createExpression(sql, new ParsingOptions(AS_DOUBLE)), false);
        RowExpression after = getRoundTrip(sql, false);
        assertEquals(before, after);
    }

    @Test
    public void testHllLiteral()
    {
        RowExpression rowExpression = getRoundTrip("empty_approx_set()", true);
        assertTrue(rowExpression instanceof ConstantExpression);
        Object value = ((ConstantExpression) rowExpression).getValue();
        assertEquals(HyperLogLog.newInstance((Slice) value).cardinality(), 0);
    }

    private void assertLiteral(@Language("SQL") String sql, ConstantExpression expected)
    {
        assertEquals(getRoundTrip(sql, true), expected);
    }

    private void assertStringLiteral(@Language("SQL") String sql, String expectedString, Type expectedType)
    {
        RowExpression roundTrip = getRoundTrip(sql, true);
        assertTrue(roundTrip instanceof ConstantExpression);
        String roundTripValue = ((Slice) ((ConstantExpression) roundTrip).getValue()).toStringUtf8();
        Type roundTripType = roundTrip.getType();
        assertEquals(roundTripValue, expectedString);
        assertEquals(roundTripType, expectedType);
    }

    private RowExpression getRoundTrip(String sql, boolean optimize)
    {
        Expression parsedExpression = new SqlParser().createExpression(sql, new ParsingOptions(AS_DOUBLE));
        parsedExpression = planExpression(PLANNER_CONTEXT, TEST_SESSION, TypeProvider.empty(), parsedExpression);
        RowExpression rowExpression = translate(parsedExpression, optimize);
        String json = codec.toJson(rowExpression);
        return codec.fromJson(json);
    }

    private ResolvedFunction operator(OperatorType operatorType, Type... types)
    {
        return metadata.resolveOperator(TEST_SESSION, operatorType, Arrays.asList(types));
    }

    private ResolvedFunction function(String name, Type... types)
    {
        return metadata.resolveFunction(TEST_SESSION, QualifiedName.of(name), fromTypes(types));
    }

    private JsonCodec<RowExpression> getJsonCodec()
            throws Exception
    {
        Module module = binder -> {
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule());
            configBinder(binder).bindConfig(FeaturesConfig.class);

            TypeManager functionManager = PLANNER_CONTEXT.getTypeManager();
            binder.bind(TypeManager.class).toInstance(functionManager);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            newSetBinder(binder, Type.class);

            binder.bind(BlockEncodingSerde.class).to(TestingBlockEncodingSerde.class).in(Scopes.SINGLETON);
            newSetBinder(binder, BlockEncoding.class);
            jsonBinder(binder).addSerializerBinding(Block.class).to(BlockJsonSerde.Serializer.class);
            jsonBinder(binder).addDeserializerBinding(Block.class).to(BlockJsonSerde.Deserializer.class);
            jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
            jsonBinder(binder).addDeserializerBinding(TypeSignature.class).to(TypeSignatureDeserializer.class);
            jsonBinder(binder).addKeyDeserializerBinding(TypeSignature.class).to(TypeSignatureKeyDeserializer.class);
            jsonCodecBinder(binder).bindJsonCodec(RowExpression.class);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        return injector.getInstance(new Key<JsonCodec<RowExpression>>() {});
    }

    private RowExpression translate(Expression expression, boolean optimize)
    {
        Map<NodeRef<Expression>, Type> types = ExpressionTestUtils.getTypes(TEST_SESSION, PLANNER_CONTEXT, TypeProvider.empty(), expression);
        RowExpression rowExpression = SqlToRowExpressionTranslator.translate(expression, types, ImmutableMap.of(), PLANNER_CONTEXT.getMetadata(), PLANNER_CONTEXT.getFunctionManager(), TEST_SESSION, optimize);
        if (optimize) {
            ExpressionOptimizer optimizer = new ExpressionOptimizer(metadata, PLANNER_CONTEXT.getFunctionManager(), TEST_SESSION);
            return optimizer.optimize(rowExpression);
        }
        return rowExpression;
    }
}
