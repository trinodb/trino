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

public class TestBasicSQLs
        extends SQLTester
{
    @Test
    public void testUse()
    {
        String sql = "USE hive.tmp";
        checkASTNode(sql);
    }

    @Test
    public void testSetSession()
    {
        String sql = "SET SESSION foo=true";
        checkASTNode(sql);
    }

    @Test
    public void testFuncCall()
    {
        String sql = "SELECT func1(a)";

        checkASTNode(sql);
    }

    @Test
    public void testFuncNestedCall()
    {
        String sql = "SELECT func1(func2(a))";

        checkASTNode(sql);
    }

    @Test
    public void testSelectExpression()
    {
        String sql = "SELECT 1 + 2 + 3";

        checkASTNode(sql);
    }

    @Test
    public void testColumnAlias()
    {
        String sql = "SELECT 1 as cnt";

        checkASTNode(sql);
    }

    @Test
    public void testSelectFrom()
    {
        String sql = "SELECT a from tb1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectAllFrom()
    {
        String sql = "SELECT * from tb1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectAllFrom1()
    {
        String sql = "SELECT a, * from tb1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectAllWithQualifierFrom()
    {
        String sql = "SELECT tb1.* from tb1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectFromTableAlias()
    {
        String sql = "SELECT t.a from tb1 t";

        checkASTNode(sql);
    }

    @Test
    public void testSelectFromWithDBAndTable()
    {
        String sql = "SELECT a from db1.tb1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectFromWithDistinct()
    {
        String sql = "SELECT distinct a from tb1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectFromWithBacktick()
    {
        String prestoSql = "SELECT a FROM tb1";
        String hiveSql = "SELECT `a` FROM tb1";

        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testWhereClause()
    {
        String sql = "SELECT a, b from tb1 where c > 10 and c <> 'hello' or (d = true and e is not null)";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseIsNotNull()
    {
        String sql = "SELECT a from tb1 where e is not null";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseIsNull()
    {
        String sql = "SELECT a from tb1 where e is null";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseBetween()
    {
        String sql = "SELECT a from tb1 where e BETWEEN start1 and end2";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseNotBetween()
    {
        String sql = "SELECT a from tb1 where e NOT BETWEEN start1 and end2";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseIn()
    {
        String sql = "SELECT a from tb1 where e in(1, 2)";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseNotIn()
    {
        String sql = "SELECT a from tb1 where e not in(1, 2)";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseLike()
    {
        String sql = "SELECT a from tb1 where e like 'a%'";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseNotLike()
    {
        String sql = "SELECT a from tb1 where e not like 'a%'";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseDistinct()
    {
        String sql = "SELECT a from tb1 where e is distinct from end_day";

        checkASTNode(sql);
    }

    @Test
    public void testWhereClauseNotDistinct()
    {
        String sql = "SELECT a from tb1 where e is not distinct from end_day";

        checkASTNode(sql);
    }

    @Test
    public void testLimit()
    {
        String sql = "SELECT a from tb1 limit 10";

        checkASTNode(sql);
    }

    @Test
    public void testOrderBy()
    {
        String sql = "SELECT a from tb1 order by t desc";

        checkASTNode(sql);
    }

    @Test
    public void testSelectCountStar()
    {
        String sql = "SELECT count(*) from tb1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectCountNULL()
    {
        String sql = "SELECT count(NULL) from tb1";

        checkASTNode(sql);
    }

    @Test
    public void testSelectCountOne()
    {
        String sql = "SELECT count(1) from tb1";

        checkASTNode(sql);
    }

    @Test
    public void testNumColumns()
    {
        String hiveSql = "SELECT 123_column from tb1";

        runHiveSQL(hiveSql);
    }

    @Test
    public void testNumColumns1()
    {
        String hiveSql = "SELECT `123_column` from tb1";

        runHiveSQL(hiveSql);
    }

    @Test
    public void testNumColumns2()
    {
        String hiveSql = "SELECT `ccc123column` from tb1";

        runHiveSQL(hiveSql);
    }

    @Test
    public void testNumTable()
    {
        String hiveSql = "SELECT `123_column` from 123_tb1";

        runHiveSQL(hiveSql);
    }

    @Test
    public void testNumTable1()
    {
        String hiveSql = "SELECT `123_column` from `123_tb1`";

        runHiveSQL(hiveSql);
    }

    @Test
    public void testQuotedTable()
    {
        String hiveSql = "SELECT `column` from `m.tb1`";
        String prestoSql = "SELECT column from m.tb1";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testCountMultiColumns()
    {
        String hiveSql = "SELECT count(x, y, z) from t";
        String prestoSql = "SELECT count(row(x, y, z)) from t";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testCountDistinctMultiColumns()
    {
        String hiveSql = "SELECT count(distinct x, y, z) from t";
        String prestoSql = "SELECT count(distinct row(x, y, z)) from t";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testConcatPipe()
    {
        String hiveSql = "SELECT a||b from t";
        checkASTNode(hiveSql);
    }

    @Test
    public void testCreateTempFunction()
    {
        String hiveSql = "CREATE TEMPORARY FUNCTION ntohl AS 'Ntohl'";
        runHiveSQL(hiveSql);
    }

    @Test
    public void testDelete()
    {
        String sql = "delete from tbl where day='2019-10-10'";
        checkASTNode(sql);
    }

    @Test
    public void testTruncate()
    {
        String prestoSql = "delete from tbl where day='2019-10-10' and hour='00' and event_id='010101'";
        String hiveSql = "truncate table tbl partition (day='2019-10-10',hour='00',event_id='010101')";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testTruncate1()
    {
        String prestoSql = "delete from tbl where day='2019-10-10'";
        String hiveSql = "truncate table tbl partition (day='2019-10-10')";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testTruncate2()
    {
        String prestoSql = "delete from tbl";
        String hiveSql = "truncate table tbl";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testMultiParentheses()
    {
        String prestoSql = "((((select 1))))";
        String hiveSql = "((((select 1))))";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testMultiParentheses1()
    {
        String prestoSql = "select 1";
        String hiveSql = "select 1";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testMultiParentheses2()
    {
        String prestoSql = "(select 1)";
        String hiveSql = "(select 1)";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testInsertMultiParentheses()
    {
        String prestoSql = "insert into  t (select 1 as m)";
        String hiveSql = "insert into  t (select 1 as m)";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testInsertMultiParentheses2()
    {
        String prestoSql = "insert into  t ((select 1 as m))";
        String hiveSql = "insert into  t ((select 1 as m))";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testInsertMultiParentheses1()
    {
        String prestoSql = "insert into  t select 1 as m";
        String hiveSql = "insert into  t select 1 as m";
        checkASTNode(prestoSql, hiveSql);
    }

    @Test
    public void testInsertIntoValues()
    {
        String sql = "insert into tbl values ('a','b'),('c','d')";
        checkASTNode(sql);
    }

    @Test
    public void testSample()
    {
        String sql = "select * from tbl TABLESAMPLE BERNOULLI(25)";
        checkASTNode(sql);
    }

    @Test
    public void testSample1()
    {
        String sql = "select * from tbl TABLESAMPLE SYSTEM(25)";
        checkASTNode(sql);
    }

    @Test
    public void testSample2()
    {
        String sql = "select * from tbl t TABLESAMPLE SYSTEM(25)";
        checkASTNode(sql);
    }

    @Test
    public void testSample3()
    {
        String sql = "select * from (select uid from t)tbl TABLESAMPLE SYSTEM(25)";
        checkASTNode(sql);
    }

    @Test
    public void testSelectColumn()
    {
        String sql = "select a.b from tbl";
        checkASTNode(sql);
    }

    @Test
    public void testWhere()
    {
        String sql = "select a.b from tbl where a=b and c=d and m=n";
        checkASTNode(sql);
    }

    @Test
    public void testQuote()
    {
        String hiveSql = "alter table tbl add columns( `event` struct<`time`:bigint,`log_extra`:map<string,string>>)";

        runHiveSQL(hiveSql);
    }
}
