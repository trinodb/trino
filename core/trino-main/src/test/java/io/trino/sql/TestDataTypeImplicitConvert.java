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

import io.trino.metadata.Metadata;
import io.trino.sql.tree.Expression;
import org.testng.annotations.Test;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.sql.ExpressionTestUtils.analyzeExpression;
import static io.trino.sql.ExpressionTestUtils.assertExpressionEquals;
import static io.trino.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestDataTypeImplicitConvert
{
    private final Metadata metadata = createTestMetadataManager();

    @Test
    public void testIntVarcharComparisonExp()
    {
        Expression exp1 = expression("1='1'");
        analyzeExpression(metadata, exp1);
        assertExpressionEquals(expression(exp1.toString()), expression("(1 = TRY(CAST('1' AS integer)))"));

        Expression exp2 = expression("1!='1'");
        analyzeExpression(metadata, exp2);
        assertExpressionEquals(expression(exp2.toString()), expression("(1 <> TRY(CAST('1' AS integer)))"));

        Expression exp3 = expression("1>'1'");
        analyzeExpression(metadata, exp3);
        assertExpressionEquals(expression(exp3.toString()), expression("(1 > TRY(CAST('1' AS integer)))"));

        Expression exp4 = expression("1>='1'");
        analyzeExpression(metadata, exp4);
        assertExpressionEquals(expression(exp4.toString()), expression("(1 >= TRY(CAST('1' AS integer)))"));

        Expression exp5 = expression("1<'1'");
        analyzeExpression(metadata, exp5);
        assertExpressionEquals(expression(exp5.toString()), expression("(1 < TRY(CAST('1' AS integer)))"));

        Expression exp6 = expression("1<='1'");
        analyzeExpression(metadata, exp6);
        assertExpressionEquals(expression(exp6.toString()), expression("(1 <= TRY(CAST('1' AS integer)))"));
    }

    @Test
    public void testBigIntVarcharComparisonExp()
    {
        Expression exp1 = expression("100000000000000='1'");
        analyzeExpression(metadata, exp1);
        assertExpressionEquals(expression(exp1.toString()), expression("(100000000000000 = TRY(CAST('1' AS bigint)))"));

        Expression exp2 = expression("100000000000000!='1'");
        analyzeExpression(metadata, exp2);
        assertExpressionEquals(expression(exp2.toString()), expression("(100000000000000 <> TRY(CAST('1' AS bigint)))"));

        Expression exp3 = expression("100000000000000>'1'");
        analyzeExpression(metadata, exp3);
        assertExpressionEquals(expression(exp3.toString()), expression("(100000000000000 > TRY(CAST('1' AS bigint)))"));

        Expression exp4 = expression("100000000000000>='1'");
        analyzeExpression(metadata, exp4);
        assertExpressionEquals(expression(exp4.toString()), expression("(100000000000000 >= TRY(CAST('1' AS bigint)))"));

        Expression exp5 = expression("100000000000000<'1'");
        analyzeExpression(metadata, exp5);
        assertExpressionEquals(expression(exp5.toString()), expression("(100000000000000 < TRY(CAST('1' AS bigint)))"));

        Expression exp6 = expression("100000000000000<='1'");
        analyzeExpression(metadata, exp6);
        assertExpressionEquals(expression(exp6.toString()), expression("(100000000000000 <= TRY(CAST('1' AS bigint)))"));
    }

    @Test
    public void testDoubleVarcharComparisonExp()
    {
        Expression exp1 = expression("1.12e0 ='1'");
        analyzeExpression(metadata, exp1);
        assertExpressionEquals(expression(exp1.toString()), expression("(1.12E0 = TRY(CAST('1' AS double)))"));

        Expression exp2 = expression("1.12e0!='1'");
        analyzeExpression(metadata, exp2);
        assertExpressionEquals(expression(exp2.toString()), expression("(1.12E0 <> TRY(CAST('1' AS double)))"));

        Expression exp3 = expression("1.12e0>'1'");
        analyzeExpression(metadata, exp3);
        assertExpressionEquals(expression(exp3.toString()), expression("(1.12E0 > TRY(CAST('1' AS double)))"));

        Expression exp4 = expression("1.12e0>='1'");
        analyzeExpression(metadata, exp4);
        assertExpressionEquals(expression(exp4.toString()), expression("(1.12E0 >= TRY(CAST('1' AS double)))"));

        Expression exp5 = expression("1.12e0<'1'");
        analyzeExpression(metadata, exp5);
        assertExpressionEquals(expression(exp5.toString()), expression("(1.12E0 < TRY(CAST('1' AS double)))"));

        Expression exp6 = expression("1.12e0<='1'");
        analyzeExpression(metadata, exp6);
        assertExpressionEquals(expression(exp6.toString()), expression("(1.12E0 <= TRY(CAST('1' AS double)))"));
    }

    @Test
    public void testIntVarcharArithmeticBinaryExp()
    {
        Expression exp1 = expression("1+'1'");
        analyzeExpression(metadata, exp1);
        assertExpressionEquals(expression(exp1.toString()), expression("(1 + TRY(CAST('1' AS integer)))"));

        Expression exp2 = expression("1-'1'");
        analyzeExpression(metadata, exp2);
        assertExpressionEquals(expression(exp2.toString()), expression("(1 - TRY(CAST('1' AS integer)))"));

        Expression exp3 = expression("1*'1'");
        analyzeExpression(metadata, exp3);
        assertExpressionEquals(expression(exp3.toString()), expression("(1 * TRY(CAST('1' AS integer)))"));

        Expression exp4 = expression("1/'1'");
        analyzeExpression(metadata, exp4);
        assertExpressionEquals(expression(exp4.toString()), expression("(1 / TRY(CAST('1' AS integer)))"));

        Expression exp5 = expression("1%'1'");
        analyzeExpression(metadata, exp5);
        assertExpressionEquals(expression(exp5.toString()), expression("(1 % TRY(CAST('1' AS integer)))"));
    }

    @Test
    public void testBigintVarcharArithmeticBinaryExp()
    {
        Expression exp1 = expression("100000000000000+'1'");
        analyzeExpression(metadata, exp1);
        assertExpressionEquals(expression(exp1.toString()), expression("(100000000000000 + TRY(CAST('1' AS bigint)))"));

        Expression exp2 = expression("100000000000000-'1'");
        analyzeExpression(metadata, exp2);
        assertExpressionEquals(expression(exp2.toString()), expression("(100000000000000 - TRY(CAST('1' AS bigint)))"));

        Expression exp3 = expression("100000000000000*'1'");
        analyzeExpression(metadata, exp3);
        assertExpressionEquals(expression(exp3.toString()), expression("(100000000000000 * TRY(CAST('1' AS bigint)))"));

        Expression exp4 = expression("100000000000000/'1'");
        analyzeExpression(metadata, exp4);
        assertExpressionEquals(expression(exp4.toString()), expression("(100000000000000 / TRY(CAST('1' AS bigint)))"));

        Expression exp5 = expression("100000000000000%'1'");
        analyzeExpression(metadata, exp5);
        assertExpressionEquals(expression(exp5.toString()), expression("(100000000000000 % TRY(CAST('1' AS bigint)))"));
    }

    @Test
    public void testDoubleVarcharArithmeticBinaryExp()
    {
        Expression exp1 = expression("1.12E0 + '1'");
        analyzeExpression(metadata, exp1);
        assertExpressionEquals(expression(exp1.toString()), expression("(1.12E0 + TRY(CAST('1' AS double)))"));

        Expression exp2 = expression("1.12E0 - '1'");
        analyzeExpression(metadata, exp2);
        assertExpressionEquals(expression(exp2.toString()), expression("(1.12E0 - TRY(CAST('1' AS double)))"));

        Expression exp3 = expression("1.12E0 * '1'");
        analyzeExpression(metadata, exp3);
        assertExpressionEquals(expression(exp3.toString()), expression("(1.12E0 * TRY(CAST('1' AS double)))"));

        Expression exp4 = expression("1.12E0 / '1'");
        analyzeExpression(metadata, exp4);
        assertExpressionEquals(expression(exp4.toString()), expression("(1.12E0 / TRY(CAST('1' AS double)))"));

        Expression exp5 = expression("1.12E0 %'1'");
        analyzeExpression(metadata, exp5);
        assertExpressionEquals(expression(exp5.toString()), expression("(1.12E0 % TRY(CAST('1' AS double)))"));
    }

    @Test
    public void testIntVarcharInExp()
    {
        Expression exp1 = expression("1 in('1','2','3')");
        analyzeExpression(metadata, exp1);
        assertExpressionEquals(expression(exp1.toString()), expression("(1 IN (TRY(CAST('1' AS integer)), TRY(CAST('2' AS integer)), TRY(CAST('3' AS integer))))"));

        Expression exp2 = expression("'1' in(1,2,3)");
        analyzeExpression(metadata, exp2);
        assertExpressionEquals(expression(exp2.toString()), expression("('1' IN (TRY(CAST(1 AS varchar)), TRY(CAST(2 AS varchar)), TRY(CAST(3 AS varchar))))"));

        Expression exp3 = expression("1 in(1,'2',3)");
        analyzeExpression(metadata, exp3);
        assertExpressionEquals(expression(exp3.toString()), expression("(1 IN (1, TRY(CAST('2' AS integer)), 3))"));
    }

    @Test
    public void testBigintVarcharInExp()
    {
        Expression exp1 = expression("100000000000000 in('1','2','3')");
        analyzeExpression(metadata, exp1);
        assertExpressionEquals(expression(exp1.toString()), expression("(100000000000000 IN (TRY(CAST('1' AS bigint)), TRY(CAST('2' AS bigint)), TRY(CAST('3' AS bigint))))"));

        Expression exp2 = expression("'1' in(100000000000000,200000000000000,300000000000000)");
        analyzeExpression(metadata, exp2);
        assertExpressionEquals(expression(exp2.toString()), expression("('1' IN (TRY(CAST(100000000000000 AS varchar)), TRY(CAST(200000000000000 AS varchar)), TRY(CAST(300000000000000 AS varchar))))"));

        Expression exp3 = expression("100000000000000 in(100000000000000,'2',200000000000000)");
        analyzeExpression(metadata, exp3);
        assertExpressionEquals(expression(exp3.toString()), expression("(100000000000000 IN (100000000000000, TRY(CAST('2' AS bigint)), 200000000000000))"));
    }

    @Test
    public void testDoubleVarcharInExp()
    {
        Expression exp1 = expression("1.12E0 in('1','2','3')");
        analyzeExpression(metadata, exp1);
        assertExpressionEquals(expression(exp1.toString()), expression("(1.12E0 IN (TRY(CAST('1' AS double)), TRY(CAST('2' AS double)), TRY(CAST('3' AS double))))"));

        Expression exp2 = expression("'1.12' in(1.1E0, 1.2E0, 1.3E0)");
        analyzeExpression(metadata, exp2);
        assertExpressionEquals(expression(exp2.toString()), expression("('1.12' IN (TRY(CAST(1.1E0 AS varchar)), TRY(CAST(1.2E0 AS varchar)), TRY(CAST(1.3E0 AS varchar))))"));

        Expression exp3 = expression("1.1E0 in(1.1E0,'2', 1.2E0)");
        analyzeExpression(metadata, exp3);
        assertExpressionEquals(expression(exp3.toString()), expression("(1.1E0 IN (1.1E0, TRY(CAST('2' AS double)), 1.2E0))"));
    }

    @Test
    public void testIntVarcharBetweenExp()
    {
        Expression exp1 = expression("1 between '1' and '2'");
        analyzeExpression(metadata, exp1);
        assertExpressionEquals(expression(exp1.toString()), expression("(1 BETWEEN TRY(CAST('1' AS integer)) AND TRY(CAST('2' AS integer)))"));

        Expression exp2 = expression("'1' between 1 and 2");
        analyzeExpression(metadata, exp2);
        assertExpressionEquals(expression(exp2.toString()), expression("('1' BETWEEN TRY(CAST(1 AS varchar)) AND TRY(CAST(2 AS varchar)))"));

        Expression exp3 = expression("1 between '1' and 2");
        analyzeExpression(metadata, exp3);
        assertExpressionEquals(expression(exp3.toString()), expression("(1 BETWEEN TRY(CAST('1' AS integer)) AND 2)"));
    }

    @Test
    public void testBigintVarcharBetweenExp()
    {
        Expression exp1 = expression("100000000000000 between '1' and '2'");
        analyzeExpression(metadata, exp1);
        assertExpressionEquals(expression(exp1.toString()), expression("(100000000000000 BETWEEN TRY(CAST('1' AS bigint)) AND TRY(CAST('2' AS bigint)))"));

        Expression exp2 = expression("'1' between 100000000000000 and 200000000000000");
        analyzeExpression(metadata, exp2);
        assertExpressionEquals(expression(exp2.toString()), expression("('1' BETWEEN TRY(CAST(100000000000000 AS varchar)) AND TRY(CAST(200000000000000 AS varchar)))"));

        Expression exp3 = expression("100000000000000 between '1' and 200000000000000");
        analyzeExpression(metadata, exp3);
        assertExpressionEquals(expression(exp3.toString()), expression("(100000000000000 BETWEEN TRY(CAST('1' AS bigint)) AND 200000000000000)"));
    }

    @Test
    public void testDoubleVarcharBetweenExp()
    {
        Expression exp1 = expression("1.12E0 between '1' and '2'");
        analyzeExpression(metadata, exp1);
        assertExpressionEquals(expression(exp1.toString()), expression("(1.12E0 BETWEEN TRY(CAST('1' AS double)) AND TRY(CAST('2' AS double)))"));

        Expression exp2 = expression("'1.12' between 100000000000000 and 200000000000000");
        analyzeExpression(metadata, exp2);
        assertExpressionEquals(expression(exp2.toString()), expression("('1.12' BETWEEN TRY(CAST(100000000000000 AS varchar)) AND TRY(CAST(200000000000000 AS varchar)))"));

        Expression exp3 = expression("1.12E0 between '1' and 1.13E0");
        analyzeExpression(metadata, exp3);
        assertExpressionEquals(expression(exp3.toString()), expression("(1.12E0 BETWEEN TRY(CAST('1' AS double)) AND 1.13E0)"));
    }
}
