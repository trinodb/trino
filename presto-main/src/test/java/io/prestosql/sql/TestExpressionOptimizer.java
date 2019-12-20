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
package io.prestosql.sql;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.relational.CallExpression;
import io.prestosql.sql.relational.ConstantExpression;
import io.prestosql.sql.relational.RowExpression;
import io.prestosql.sql.relational.SpecialForm;
import io.prestosql.sql.relational.optimizer.ExpressionOptimizer;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.block.BlockAssertions.toValues;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY_NAME;
import static io.prestosql.operator.scalar.JsonStringToMapCast.JSON_STRING_TO_MAP_NAME;
import static io.prestosql.operator.scalar.JsonStringToRowCast.JSON_STRING_TO_ROW_NAME;
import static io.prestosql.spi.function.OperatorType.ADD;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.field;
import static io.prestosql.sql.relational.SpecialForm.Form.IF;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.util.StructuralTestUtil.mapType;
import static org.testng.Assert.assertEquals;

public class TestExpressionOptimizer
{
    private Metadata metadata;
    private ExpressionOptimizer optimizer;

    @BeforeClass
    public void setUp()
    {
        metadata = createTestMetadataManager();
        optimizer = new ExpressionOptimizer(metadata, TEST_SESSION);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        optimizer = null;
    }

    @Test(timeOut = 10_000)
    public void testPossibleExponentialOptimizationTime()
    {
        RowExpression expression = constant(1L, BIGINT);
        for (int i = 0; i < 100; i++) {
            expression = new CallExpression(
                    metadata.resolveOperator(ADD, ImmutableList.of(BIGINT, BIGINT)),
                    BIGINT,
                    ImmutableList.of(expression, constant(1L, BIGINT)));
        }
        optimizer.optimize(expression);
    }

    @Test
    public void testIfConstantOptimization()
    {
        assertEquals(optimizer.optimize(ifExpression(constant(true, BOOLEAN), 1L, 2L)), constant(1L, BIGINT));
        assertEquals(optimizer.optimize(ifExpression(constant(false, BOOLEAN), 1L, 2L)), constant(2L, BIGINT));
        assertEquals(optimizer.optimize(ifExpression(constant(null, BOOLEAN), 1L, 2L)), constant(2L, BIGINT));

        RowExpression condition = new CallExpression(
                metadata.resolveOperator(EQUAL, ImmutableList.of(BIGINT, BIGINT)),
                BOOLEAN,
                ImmutableList.of(constant(3L, BIGINT), constant(3L, BIGINT)));
        assertEquals(optimizer.optimize(ifExpression(condition, 1L, 2L)), constant(1L, BIGINT));
    }

    @Test
    public void testCastWithJsonParseOptimization()
    {
        ResolvedFunction jsonParseFunction = metadata.resolveFunction(QualifiedName.of("json_parse"), fromTypes(VARCHAR));

        // constant
        ResolvedFunction jsonCastFunction = metadata.getCoercion(JSON, new ArrayType(INTEGER));
        RowExpression jsonCastExpression = new CallExpression(jsonCastFunction, new ArrayType(INTEGER), ImmutableList.of(call(jsonParseFunction, JSON, constant(utf8Slice("[1, 2]"), VARCHAR))));
        RowExpression resultExpression = optimizer.optimize(jsonCastExpression);
        assertInstanceOf(resultExpression, ConstantExpression.class);
        Object resultValue = ((ConstantExpression) resultExpression).getValue();
        assertInstanceOf(resultValue, IntArrayBlock.class);
        assertEquals(toValues(INTEGER, (IntArrayBlock) resultValue), ImmutableList.of(1, 2));

        // varchar to array
        testCastWithJsonParseOptimization(jsonParseFunction, new ArrayType(VARCHAR), JSON_STRING_TO_ARRAY_NAME);

        // varchar to map
        testCastWithJsonParseOptimization(jsonParseFunction, mapType(INTEGER, VARCHAR), JSON_STRING_TO_MAP_NAME);

        // varchar to row
        testCastWithJsonParseOptimization(jsonParseFunction, RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT)), JSON_STRING_TO_ROW_NAME);
    }

    private void testCastWithJsonParseOptimization(ResolvedFunction jsonParseFunction, Type targetType, String jsonStringToRowName)
    {
        ResolvedFunction jsonCastFunction = metadata.getCoercion(JSON, targetType);
        RowExpression jsonCastExpression = new CallExpression(jsonCastFunction, targetType, ImmutableList.of(call(jsonParseFunction, JSON, field(1, VARCHAR))));
        RowExpression resultExpression = optimizer.optimize(jsonCastExpression);
        assertEquals(
                resultExpression,
                call(
                        metadata.getCoercion(QualifiedName.of(jsonStringToRowName), VARCHAR, targetType),
                        targetType,
                        field(1, VARCHAR)));
    }

    private static RowExpression ifExpression(RowExpression condition, long trueValue, long falseValue)
    {
        return new SpecialForm(IF, BIGINT, ImmutableList.of(condition, constant(trueValue, BIGINT), constant(falseValue, BIGINT)));
    }
}
