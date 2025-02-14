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
package io.trino.sql.parser;

import com.google.common.io.Resources;
import io.trino.sql.SqlFormatter;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Statement;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;

import static io.trino.sql.testing.TreeAssertions.assertFormattedSql;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStatementBuilder
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testPreparedGrantWithQuotes()
    {
        printStatement("prepare p from grant select on table hive.test.\"case\" to role test");
        printStatement("prepare p from grant select on hive.test.\"case\" to role test");
        printStatement("prepare p from grant select on table hive.test.\"case\" to role \"case\"");
    }

    @Test
    public void testStatementBuilder()
    {
        printStatement("select * from foo");
        printStatement("explain select * from foo");
        printStatement("explain (type distributed, format graphviz) select * from foo");

        printStatement("select * from foo /* end */");
        printStatement("/* start */ select * from foo");
        printStatement("/* start */ select * /* middle */ from foo /* end */");
        printStatement("-- start\nselect * -- junk\n-- hi\nfrom foo -- done");

        printStatement("select * from foo a (x, y, z)");

        printStatement("select *, 123, * from foo");

        printStatement("select show from foo");
        printStatement("select extract(day from x), extract(dow from x) from y");

        printStatement("select 1 + 13 || '15' from foo");

        printStatement("select x is distinct from y from foo where a is not distinct from b");

        printStatement("select x[1] from my_table");
        printStatement("select x[1][2] from my_table");
        printStatement("select x[cast(10 * sin(x) as bigint)] from my_table");

        printStatement("select * from (select * from (select * from t) x) y");
        printStatement("select * from (select * from (table t) x) y");

        printStatement("select * from t x tablesample system (10)");
        printStatement("select * from (t x tablesample system (10)) y");
        printStatement("select * from (t tablesample system (10)) tablesample system (10)");
        printStatement("select * from (t x tablesample system (10)) y tablesample system (10)");

        printStatement("select * from (((select q)))");
        printStatement("select * from (select q) x");
        printStatement("select * from ((select q) x) y");
        printStatement("select * from (((select q) x) y) z");

        printStatement("select * from unnest(t.my_array)");
        printStatement("select * from unnest(array[1, 2, 3])");
        printStatement("select x from unnest(array[1, 2, 3]) t(x)");
        printStatement("select * from users cross join unnest(friends)");
        printStatement("select id, friend from users cross join unnest(friends) t(friend)");
        printStatement("select * from unnest(t.my_array) with ordinality");
        printStatement("select * from unnest(array[1, 2, 3]) with ordinality");
        printStatement("select x from unnest(array[1, 2, 3]) with ordinality t(x)");
        printStatement("select * from users cross join unnest(friends) with ordinality");
        printStatement("select id, friend from users cross join unnest(friends) with ordinality t(friend)");

        printStatement("select count(*) x from src group by k, v");
        printStatement("select count(*) x from src group by cube (k, v)");
        printStatement("select count(*) x from src group by rollup (k, v)");
        printStatement("select count(*) x from src group by grouping sets ((k, v))");
        printStatement("select count(*) x from src group by grouping sets ((k, v), (v))");
        printStatement("select count(*) x from src group by grouping sets (k, v, k)");

        printStatement("select count(*) filter (where x > 4) y from t");
        printStatement("select sum(x) filter (where x > 4) y from t");
        printStatement("select sum(x) filter (where x > 4) y, sum(x) filter (where x < 2) z from t");
        printStatement("select sum(distinct x) filter (where x > 4) y, sum(x) filter (where x < 2) z from t");
        printStatement("select sum(x) filter (where x > 4) over (partition by y) z from t");

        printStatement("" +
                "select depname, empno, salary\n" +
                ", count(*) over ()\n" +
                ", avg(salary) over (partition by depname)\n" +
                ", rank() over (partition by depname order by salary desc)\n" +
                ", sum(salary) over (order by salary rows unbounded preceding)\n" +
                ", sum(salary) over (partition by depname order by salary rows between current row and 3 following)\n" +
                ", sum(salary) over (partition by depname order by salary rows between current row and empno following)\n" +
                ", sum(salary) over (partition by depname range unbounded preceding)\n" +
                ", sum(salary) over (rows between 2 preceding and unbounded following)\n" +
                "from emp");

        printStatement("" +
                "with a (id) as (with x as (select 123 from z) select * from x) " +
                "   , b (id) as (select 999 from z) " +
                "select * from a join b using (id)");

        printStatement("with recursive t as (select * from x) select * from t");

        printStatement("select * from information_schema.tables");

        printStatement("show catalogs");

        printStatement("show schemas");
        printStatement("show schemas from sys");

        printStatement("show tables");
        printStatement("show tables from information_schema");
        printStatement("show tables like '%'");
        printStatement("show tables from information_schema like '%'");

        printStatement("show functions");

        printStatement("select cast('123' as bigint), try_cast('foo' as bigint)");

        printStatement("select * from a.b.c");
        printStatement("select * from a.b.c.e.f.g");

        printStatement("select \"TOTALPRICE\" \"my price\" from \"$MY\"\"ORDERS\"");

        printStatement("select * from foo tablesample system (10+1)");
        printStatement("select * from foo tablesample system (10) join bar tablesample bernoulli (30) on a.id = b.id");
        printStatement("select * from foo tablesample system (10) join bar tablesample bernoulli (30) on not(a.id > b.id)");

        printStatement("create table foo as (select * from abc)");
        printStatement("create table if not exists foo as (select * from abc)");
        printStatement("create table foo with (a = 'apple', b = 'banana') as select * from abc");
        printStatement("create table foo comment 'test' with (a = 'apple') as select * from abc");
        printStatement("create table foo as select * from abc WITH NO DATA");

        printStatement("create table foo as (with t(x) as (values 1) select x from t)");
        printStatement("create table if not exists foo as (with t(x) as (values 1) select x from t)");
        printStatement("create table foo as (with t(x) as (values 1) select x from t) WITH DATA");
        printStatement("create table if not exists foo as (with t(x) as (values 1) select x from t) WITH DATA");
        printStatement("create table foo as (with t(x) as (values 1) select x from t) WITH NO DATA");
        printStatement("create table if not exists foo as (with t(x) as (values 1) select x from t) WITH NO DATA");

        printStatement("create table foo(a) as (with t(x) as (values 1) select x from t)");
        printStatement("create table if not exists foo(a) as (with t(x) as (values 1) select x from t)");
        printStatement("create table foo(a) as (with t(x) as (values 1) select x from t) WITH DATA");
        printStatement("create table if not exists foo(a) as (with t(x) as (values 1) select x from t) WITH DATA");
        printStatement("create table foo(a) as (with t(x) as (values 1) select x from t) WITH NO DATA");
        printStatement("create table if not exists foo(a) as (with t(x) as (values 1) select x from t) WITH NO DATA");
        printStatement("drop table foo");

        printStatement("insert into foo select * from abc");

        printStatement("delete from foo");
        printStatement("delete from foo where a = b");

        printStatement("truncate table foo");

        printStatement("values ('a', 1, 2.2), ('b', 2, 3.3)");

        printStatement("table foo");
        printStatement("table foo order by x limit 10");
        printStatement("(table foo)");
        printStatement("(table foo) limit 10");
        printStatement("(table foo limit 5) limit 10");

        printStatement("select * from a limit all");
        printStatement("select * from a order by x limit all");

        printStatement("select * from a union select * from b");
        printStatement("table a union all table b");
        printStatement("(table foo) union select * from foo union (table foo order by x)");

        printStatement("table a union table b intersect table c");
        printStatement("(table a union table b) intersect table c");
        printStatement("table a union table b except table c intersect table d");
        printStatement("(table a union table b except table c) intersect table d");
        printStatement("((table a union table b) except table c) intersect table d");
        printStatement("(table a union (table b except table c)) intersect table d");
        printStatement("table a intersect table b union table c");
        printStatement("table a intersect (table b union table c)");

        printStatement("alter table foo rename to bar");
        printStatement("alter table a.b.c rename to d.e.f");

        printStatement("alter table a.b.c rename column x to y");

        printStatement("alter table foo set properties a='1'");
        printStatement("alter table a.b.c set properties a=true, b=123, c='x'");
        printStatement("alter table a.b.c set properties a=DEFAULT, b=123");

        printStatement("alter table a.b.c add column x bigint first");
        printStatement("alter table a.b.c add column x bigint after y");
        printStatement("alter table a.b.c add column x bigint last");
        printStatement("alter table a.b.c add column x bigint");

        printStatement("alter table a.b.c add column x bigint comment 'large x'");
        printStatement("alter table a.b.c add column x bigint with (weight = 2)");
        printStatement("alter table a.b.c add column x bigint comment 'xtra' with (compression = 'LZ4', special = true)");

        printStatement("alter table a.b.c drop column x");

        printStatement("alter table foo alter column x set data type bigint");
        printStatement("alter table a.b.c alter column x set data type bigint");

        printStatement("alter table foo alter column x drop not null");
        printStatement("alter table a.b.c alter column x drop not null");

        printStatement("alter materialized view foo set properties a='1'");
        printStatement("alter materialized view a.b.c set properties a=true, b=123, c='x'");
        printStatement("alter materialized view a.b.c set properties a=default, b=123");

        printStatement("create schema test");
        printStatement("create schema test authorization alice");
        printStatement("create schema test authorization alice with ( location = 'xyz' )");
        printStatement("create schema test authorization user alice");
        printStatement("create schema test authorization user alice with ( location = 'xyz' )");
        printStatement("create schema test authorization role public");
        printStatement("create schema test authorization role public with ( location = 'xyz' )");
        printStatement("create schema if not exists test");
        printStatement("create schema test with (a = 'apple', b = 123)");

        printStatement("drop schema test");
        printStatement("drop schema test cascade");
        printStatement("drop schema if exists test");
        printStatement("drop schema if exists test restrict");

        printStatement("alter schema foo rename to bar");
        printStatement("alter schema foo.bar rename to baz");

        printStatement("alter schema foo set authorization alice");
        printStatement("alter schema foo.bar set authorization USER alice");
        printStatement("alter schema foo.bar set authorization ROLE public");

        printStatement("create table test (a boolean, b bigint, c double, d varchar, e timestamp)");
        printStatement("create table test (a boolean, b bigint comment 'test')");
        printStatement("create table if not exists baz (a timestamp, b varchar)");
        printStatement("create table test (a boolean, b bigint) with (a = 'apple', b = 'banana')");
        printStatement("create table test (a boolean, b bigint) comment 'test' with (a = 'apple')");
        printStatement("create table test (a boolean with (a = 'apple', b = 'banana'), b bigint comment 'bla' with (c = 'cherry')) comment 'test' with (a = 'apple')");
        printStatement("comment on table test is 'test'");
        printStatement("comment on view test is 'test'");
        printStatement("comment on column test.a is 'test'");
        printStatement("drop table test");

        printStatement("alter table foo set authorization alice");
        printStatement("alter table foo.bar set authorization USER alice");
        printStatement("alter table foo.bar.baz set authorization ROLE public");

        printStatement("create view foo as with a as (select 123) select * from a");
        printStatement("create or replace view foo as select 123 from t");

        printStatement("drop view foo");
        printStatement("alter view foo set authorization alice");
        printStatement("alter view foo.bar set authorization USER alice");
        printStatement("alter view foo.bar.baz set authorization ROLE public");

        printStatement("insert into t select * from t");
        printStatement("insert into t (c1, c2) select * from t");

        printStatement("start transaction");
        printStatement("start transaction isolation level read uncommitted");
        printStatement("start transaction isolation level read committed");
        printStatement("start transaction isolation level repeatable read");
        printStatement("start transaction isolation level serializable");
        printStatement("start transaction read only");
        printStatement("start transaction read write");
        printStatement("start transaction isolation level read committed, read only");
        printStatement("start transaction read only, isolation level read committed");
        printStatement("start transaction read write, isolation level serializable");
        printStatement("commit");
        printStatement("commit work");
        printStatement("rollback");
        printStatement("rollback work");

        printStatement("call foo()");
        printStatement("call foo(123, a => 1, b => 'go', 456)");

        printStatement("grant select on foo to alice with grant option");
        printStatement("grant all privileges on foo to alice");
        printStatement("grant delete, select on foo to role public");
        printStatement("deny select on foo to alice");
        printStatement("deny all privileges on foo to alice");
        printStatement("deny delete, select on foo to role public");
        printStatement("deny select on schema foo to alice");
        printStatement("deny all privileges on schema foo to alice");
        printStatement("deny delete, select on schema foo to role public");
        printStatement("revoke grant option for select on foo from alice");
        printStatement("revoke all privileges on foo from alice");
        printStatement("revoke insert, delete on foo from role public");
        printStatement("show grants on table t");
        printStatement("show grants on t");
        printStatement("show grants");
        printStatement("show roles");
        printStatement("show roles from foo");
        printStatement("show current roles");
        printStatement("show current roles from foo");
        printStatement("show role grants");
        printStatement("show role grants from foo");

        printStatement("show create schema abc");
        printStatement("show create table abc");
        printStatement("show create view abc");
        printStatement("show create materialized view abc");
        printStatement("show create function abc");

        printStatement("prepare p from select * from (select * from T) \"A B\"");

        printStatement("SELECT * FROM table1 WHERE a >= ALL (VALUES 2, 3, 4)");
        printStatement("SELECT * FROM table1 WHERE a <> ANY (SELECT 2, 3, 4)");
        printStatement("SELECT * FROM table1 WHERE a = SOME (SELECT id FROM table2)");

        printStatement("" +
                "merge into inventory as i\n" +
                "using changes as c\n" +
                "on i.part = c.part\n" +
                "when matched and c.action = 'mod' then\n" +
                "update set qty = qty + c.qty\n" +
                "when matched and c.action = 'del' then delete\n" +
                "when not matched and c.action = 'new' then\n" +
                "insert (part, qty) values (c.part, c.qty)");

        printStatement("set session authorization user");
        printStatement("reset session authorization");
    }

    @Test
    public void testStringFormatter()
    {
        assertSqlFormatter("U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801'",
                "'hello测试\uDBFF\uDFFFworld编码'");
        assertSqlFormatter("'hello world'", "'hello world'");
        assertSqlFormatter("U&'!+10FFFF!6d4B!8Bd5ABC!6d4B!8Bd5' UESCAPE '!'", "'\uDBFF\uDFFF测试ABC测试'");
        assertSqlFormatter("U&'\\+10FFFF\\6D4B\\8BD5\\0041\\0042\\0043\\6D4B\\8BD5'", "'\uDBFF\uDFFF测试ABC测试'");
        assertSqlFormatter("U&'\\\\abc\\6D4B'''", "'\\abc测'''");
    }

    @Test
    public void testStatementBuilderTpch()
    {
        printTpchQuery(1, 3);
        printTpchQuery(2, 33, "part type like", "region name");
        printTpchQuery(3, "market segment", "2013-03-05");
        printTpchQuery(4, "2013-03-05");
        printTpchQuery(5, "region name", "2013-03-05");
        printTpchQuery(6, "2013-03-05", 33, 44);
        printTpchQuery(7, "nation name 1", "nation name 2");
        printTpchQuery(8, "nation name", "region name", "part type");
        printTpchQuery(9, "part name like");
        printTpchQuery(10, "2013-03-05");
        printTpchQuery(11, "nation name", 33);
        printTpchQuery(12, "ship mode 1", "ship mode 2", "2013-03-05");
        printTpchQuery(13, "comment like 1", "comment like 2");
        printTpchQuery(14, "2013-03-05");
        // query 15: views not supported
        printTpchQuery(16, "part brand", "part type like", 3, 4, 5, 6, 7, 8, 9, 10);
        printTpchQuery(17, "part brand", "part container");
        printTpchQuery(18, 33);
        printTpchQuery(19, "part brand 1", "part brand 2", "part brand 3", 11, 22, 33);
        printTpchQuery(20, "part name like", "2013-03-05", "nation name");
        printTpchQuery(21, "nation name");
        printTpchQuery(22,
                "phone 1",
                "phone 2",
                "phone 3",
                "phone 4",
                "phone 5",
                "phone 6",
                "phone 7");
    }

    private static void printStatement(String sql)
    {
        println(sql.trim());
        println("");

        Statement statement = SQL_PARSER.createStatement(sql);
        println(statement.toString());
        println("");

        println(SqlFormatter.formatSql(statement));
        println("");
        assertFormattedSql(SQL_PARSER, statement);

        println("=".repeat(60));
        println("");
    }

    private static void assertSqlFormatter(String expression, String formatted)
    {
        Expression originalExpression = SQL_PARSER.createExpression(expression);
        String real = SqlFormatter.formatSql(originalExpression);
        assertThat(real).isEqualTo(formatted);
    }

    private static void println(String s)
    {
        if (Boolean.parseBoolean(System.getProperty("printParse"))) {
            System.out.println(s);
        }
    }

    private static String getTpchQuery(int q)
    {
        return readResource("tpch/queries/" + q + ".sql");
    }

    private static void printTpchQuery(int query, Object... values)
    {
        String sql = getTpchQuery(query);

        for (int i = values.length - 1; i >= 0; i--) {
            sql = sql.replaceAll(":%s".formatted(i + 1), String.valueOf(values[i]));
        }

        assertThat(sql.matches("(?s).*:[0-9].*"))
                .as("Not all bind parameters were replaced: " + sql)
                .isFalse();

        sql = fixTpchQuery(sql);
        printStatement(sql);
    }

    private static String readResource(String name)
    {
        try {
            return Resources.toString(Resources.getResource(name), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String fixTpchQuery(String s)
    {
        s = s.replaceFirst("(?m);$", "");
        s = s.replaceAll("(?m)^:[xo]$", "");
        s = s.replaceAll("(?m)^:n -1$", "");
        s = s.replaceAll("(?m)^:n ([0-9]+)$", "LIMIT $1");
        s = s.replace("day (3)", "day"); // for query 1
        return s;
    }
}
