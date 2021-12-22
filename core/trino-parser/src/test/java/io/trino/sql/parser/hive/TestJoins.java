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

public class TestJoins
        extends SQLTester
{
    @Test
    public void testJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testInnerJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testLeftOuterJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "LEFT OUTER JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testLeftJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "LEFT JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testRightOuterJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "RIGHT OUTER JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testRightJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "RIGHT JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testFullOuterJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "FULL Outer JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testFullJoin()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate\n" +
                "FROM Orders\n" +
                "FULL JOIN Customers ON Orders.CustomerID=Customers.CustomerID" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testThreeJoins()
    {
        String sql = "" +
                "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate, Items.Name\n" +
                "FROM Orders\n" +
                "JOIN Customers ON Orders.CustomerID=Customers.CustomerID\n" +
                "JOIN Items ON Orders.ItemID=Items.ID AND Orders.ItemType=Items.Type" +
                "";

        checkASTNode(sql);
    }

    @Test
    public void testJoinsAliasedRelation()
    {
        String sql = "" +
                "SELECT * " +
                "FROM\n" +
                "  (SELECT qq\n" +
                "   FROM d.b\n" +
                "   WHERE cc='2019-11-21') t1\n" +
                "LEFT JOIN (e.a) t3 ON t1.qq=t3.qq";

        checkASTNode(sql);
    }

    @Test
    public void testJoinsAliasedRelationWithoutAlias()
    {
        String sql = "" +
                "SELECT *\n" +
                "FROM   (SELECT ccc\n" +
                "        FROM   ta1 \n" +
                "       ) \n" +
                "WHERE  ccc IN (SELECT ff \n" +
                "                       FROM ta2 where c1 > 10) ";

        checkASTNode(sql);
    }
}
