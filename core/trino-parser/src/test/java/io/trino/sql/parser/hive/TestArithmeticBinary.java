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
package io.trino.sql.parser.hive;

import io.trino.sql.ExpressionFormatter;
import io.trino.sql.parser.ParsingOptions;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import org.testng.annotations.Test;

public class TestArithmeticBinary
        extends SQLTester
{
    @Test
    public void testBase()
    {
        String sql = "SELECT x|y&z^m";

        checkASTNode(sql);
    }

    @Test
    public void testFromTable()
    {
        String sql = "SELECT x&y|z^m from m where x like 'hajksda'";

        checkASTNode(sql);
    }

    @Test
    public void testSeDe()
    {
        ParsingOptions parsingOptions = new ParsingOptions();
        String sql = "x&y|z^m+c-d/6%5 DIV m";

        Node node = new SqlParser().createExpression(sql, parsingOptions);

        String fomatedAst = ExpressionFormatter.formatExpression((Expression) node);
        Node node1 = new SqlParser().createExpression(fomatedAst, parsingOptions);
        checkASTNode(node, node1);
    }
}
