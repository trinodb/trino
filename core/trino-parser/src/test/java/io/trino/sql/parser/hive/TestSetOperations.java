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

public class TestSetOperations
        extends SQLTester
{
    @Test
    public void testUnion()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "UNION\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testUnionAll()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "UNION ALL\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testUnionDistinct()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "UNION DISTINCT\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testExcept()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "EXCEPT\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testExceptAll()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "EXCEPT ALL\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testExceptDistinct()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "EXCEPT DISTINCT\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testIntersect()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "INTERSECT\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testIntersectAll()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "INTERSECT ALL\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testIntersectDistinct()
    {
        String sql = "" +
                "SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS1\n" +
                "INTERSECT DISTINCT\n" +
                "   SELECT ID, NAME, AMOUNT, DATE\n" +
                "   FROM CUSTOMERS2\n";

        checkASTNode(sql);
    }

    @Test
    public void testBothSidesAreQuery()
    {
        String sql = "" +
                "(select a from ta)\n" +
                "union all\n" +
                "(select b from tb)";

        runHiveSQL(sql);
    }
}
