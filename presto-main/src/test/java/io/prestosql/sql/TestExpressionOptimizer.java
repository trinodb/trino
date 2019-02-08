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
import io.prestosql.metadata.FunctionHandle;
import io.prestosql.metadata.FunctionManager;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
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
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.field;
import static io.prestosql.sql.relational.SpecialForm.Form.IF;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.util.StructuralTestUtil.mapType;
import static org.testng.Assert.assertEquals;

public class TestExpressionOptimizer
{
    private static final String JSON_PARSE = "json_parse";
    private FunctionManager functionManager;
    private ExpressionOptimizer optimizer;

    @BeforeClass
    public void setUp()
    {
        MetadataManager metadata = createTestMetadataManager();
        functionManager = metadata.getFunctionManager();
        optimizer = new ExpressionOptimizer(functionManager, TEST_SESSION);
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

        FunctionHandle functionHandle = functionManager.resolveOperator(ADD, fromTypes(BIGINT, BIGINT));

        for (int i = 0; i < 100; i++) {
            expression = new CallExpression(ADD.name(), functionHandle, BIGINT, ImmutableList.of(expression, constant(1L, BIGINT)));
        }
        optimizer.optimize(expression);
    }

    @Test
    public void testIfConstantOptimization()
    {
        assertEquals(optimizer.optimize(ifExpression(constant(true, BOOLEAN), 1L, 2L)), constant(1L, BIGINT));
        assertEquals(optimizer.optimize(ifExpression(constant(false, BOOLEAN), 1L, 2L)), constant(2L, BIGINT));
        assertEquals(optimizer.optimize(ifExpression(constant(null, BOOLEAN), 1L, 2L)), constant(2L, BIGINT));

        FunctionHandle bigintEquals = functionManager.resolveOperator(EQUAL, fromTypes(BIGINT, BIGINT));
        RowExpression condition = new CallExpression(EQUAL.name(), bigintEquals, BOOLEAN, ImmutableList.of(constant(3L, BIGINT), constant(3L, BIGINT)));
        assertEquals(optimizer.optimize(ifExpression(condition, 1L, 2L)), constant(1L, BIGINT));
    }

    @Test
    public void testCastWithJsonParseOptimization()
    {
        FunctionHandle jsonParseFunction = functionManager.lookupFunction(QualifiedName.of(JSON_PARSE), fromTypes(VARCHAR));

        // constant
        FunctionHandle jsonCastSignature = functionManager.lookupCast(JSON.getTypeSignature(), parseTypeSignature("array(integer)"));
        RowExpression jsonCastExpression = new CallExpression(
                CAST.name(),
                jsonCastSignature,
                new ArrayType(INTEGER),
                ImmutableList.of(new CallExpression(JSON_PARSE, jsonParseFunction, JSON, ImmutableList.of(constant(utf8Slice("[1, 2]"), VARCHAR)))));
        RowExpression resultExpression = optimizer.optimize(jsonCastExpression);
        assertInstanceOf(resultExpression, ConstantExpression.class);
        Object resultValue = ((ConstantExpression) resultExpression).getValue();
        assertInstanceOf(resultValue, IntArrayBlock.class);
        assertEquals(toValues(INTEGER, (IntArrayBlock) resultValue), ImmutableList.of(1, 2));

        // varchar to array
        jsonCastSignature = functionManager.lookupCast(JSON.getTypeSignature(), parseTypeSignature("array(varchar)"));
        jsonCastExpression = new CallExpression(CAST.name(), jsonCastSignature, new ArrayType(VARCHAR), ImmutableList.of(new CallExpression(JSON_PARSE, jsonParseFunction, JSON, ImmutableList.of(field(1, VARCHAR)))));
        resultExpression = optimizer.optimize(jsonCastExpression);
        FunctionHandle jsonToArrayFunction = functionManager.lookupInternalCastFunction(JSON_STRING_TO_ARRAY_NAME, VARCHAR.getTypeSignature(), new ArrayType(VARCHAR).getTypeSignature());
        assertEquals(
                resultExpression,
                new CallExpression(JSON_STRING_TO_ARRAY_NAME, jsonToArrayFunction, new ArrayType(VARCHAR), ImmutableList.of(field(1, VARCHAR))));

        // varchar to map
        jsonCastSignature = functionManager.lookupCast(JSON.getTypeSignature(), parseTypeSignature("map(integer,varchar)"));
        jsonCastExpression = new CallExpression(CAST.name(), jsonCastSignature, mapType(INTEGER, VARCHAR), ImmutableList.of(new CallExpression(JSON_PARSE, jsonParseFunction, JSON, ImmutableList.of(field(1, VARCHAR)))));
        resultExpression = optimizer.optimize(jsonCastExpression);
        FunctionHandle jsonToMapFunction = functionManager.lookupInternalCastFunction(JSON_STRING_TO_MAP_NAME, VARCHAR.getTypeSignature(), mapType(INTEGER, VARCHAR).getTypeSignature());
        assertEquals(
                resultExpression,
                new CallExpression(JSON_STRING_TO_MAP_NAME, jsonToMapFunction, mapType(INTEGER, VARCHAR), ImmutableList.of(field(1, VARCHAR))));

        // varchar to row
        jsonCastSignature = functionManager.lookupCast(JSON.getTypeSignature(), parseTypeSignature("row(varchar,bigint)"));
        jsonCastExpression = new CallExpression(CAST.name(), jsonCastSignature, RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT)), ImmutableList.of(new CallExpression(JSON_PARSE, jsonParseFunction, JSON, ImmutableList.of(field(1, VARCHAR)))));
        resultExpression = optimizer.optimize(jsonCastExpression);
        FunctionHandle jsonToRowFunction = functionManager.lookupInternalCastFunction(JSON_STRING_TO_ROW_NAME, VARCHAR.getTypeSignature(), RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT)).getTypeSignature());
        assertEquals(
                resultExpression,
                new CallExpression(JSON_STRING_TO_ROW_NAME, jsonToRowFunction, RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT)), ImmutableList.of(field(1, VARCHAR))));
    }

    private static RowExpression ifExpression(RowExpression condition, long trueValue, long falseValue)
    {
        return new SpecialForm(IF, BIGINT, ImmutableList.of(condition, constant(trueValue, BIGINT), constant(falseValue, BIGINT)));
    }
}
