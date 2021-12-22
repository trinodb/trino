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

import org.testng.annotations.Test;

public class TestSubQueries
        extends SQLTester
{
    @Test
    public void testInSubQuery()
    {
        String sql = "" +
                "SELECT ProductName\n" +
                "  FROM Product \n" +
                "WHERE Id IN (SELECT ProductId \n" +
                "                FROM OrderItem\n" +
                "               WHERE Quantity > 100)\n";

        checkASTNode(sql);
    }

    @Test
    public void testExistsSubQuery()
    {
        String sql = "" +
                "SELECT ProductName\n" +
                "  FROM Product \n" +
                "WHERE EXISTS (SELECT ProductId \n" +
                "                FROM OrderItem\n" +
                "               WHERE Quantity > 100)\n";

        checkASTNode(sql);
    }

    @Test
    public void testTableSubQuery()
    {
        String sql = "" +
                "SELECT ProductName\n" +
                "FROM (\n" +
                "    SELECT ProductName \n" +
                "    FROM OrderItem\n" +
                "    WHERE Quantity > 100\n" +
                ") t1";

        checkASTNode(sql);
    }
}
