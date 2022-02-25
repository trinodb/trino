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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.dispatcher.DispatchManager;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryManager;
import io.trino.server.BasicQueryInfo;
import io.trino.testing.sql.TestTable;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.connector.informationschema.InformationSchemaTable.INFORMATION_SCHEMA;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.QueryAssertions.assertContains;
import static io.trino.testing.QueryAssertions.getTrinoExceptionCause;
import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.testing.assertions.Assert.assertEventually;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Generic test for connectors exercising connector's read and write capabilities.
 *
 * @see BaseConnectorTest
 * @deprecated Extend {@link BaseConnectorTest} instead.
 */
@Deprecated
public abstract class AbstractTestDistributedQueries
        extends AbstractTestQueries
{
    protected boolean supportsCreateSchema()
    {
        return true;
    }

    private boolean supportsDropSchema()
    {
        // A connector either supports CREATE SCHEMA and DROP SCHEMA or none of them.
        return supportsCreateSchema();
    }

    protected boolean supportsCreateTable()
    {
        return true;
    }

    protected boolean supportsInsert()
    {
        return true;
    }

    protected boolean supportsDelete()
    {
        return true;
    }

    protected boolean supportsViews()
    {
        return true;
    }

    protected boolean supportsArrays()
    {
        return true;
    }

    protected boolean supportsCommentOnTable()
    {
        return true;
    }

    protected boolean supportsCommentOnColumn()
    {
        return true;
    }

    protected boolean supportsRenameTable()
    {
        return true;
    }

    /**
     * Ensure the tests are run with {@link DistributedQueryRunner}. E.g. {@link LocalQueryRunner} takes some
     * shortcuts, not exercising certain aspects.
     */
    @Test
    public void ensureDistributedQueryRunner()
    {
        assertThat(getQueryRunner().getNodeCount()).as("query runner node count")
                .isGreaterThanOrEqualTo(3);
    }

    @Test
    public void testCreateTable()
    {
        String tableName = "test_create_" + randomTableSuffix();
        if (!supportsCreateTable()) {
            assertQueryFails("CREATE TABLE " + tableName + " (a bigint, b double, c varchar(50))", "This connector does not support creating tables");
            return;
        }

        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double, c varchar(50))");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        assertQueryFails("CREATE TABLE " + tableName + " (a bad_type)", ".* Unknown type 'bad_type' for column 'a'");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        // TODO (https://github.com/trinodb/trino/issues/5901) revert to longer name when Oracle version is updated
        tableName = "test_cr_not_exists_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b varchar(50), c double)");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " (d bigint, e varchar(50))");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        // Test CREATE TABLE LIKE
        tableName = "test_create_orig_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a bigint, b double, c varchar(50))");
        assertTrue(getQueryRunner().tableExists(getSession(), tableName));
        assertTableColumnNames(tableName, "a", "b", "c");

        String tableNameLike = "test_create_like_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableNameLike + " (LIKE " + tableName + ", d bigint, e varchar(50))");
        assertTrue(getQueryRunner().tableExists(getSession(), tableNameLike));
        assertTableColumnNames(tableNameLike, "a", "b", "c", "d", "e");

        assertUpdate("DROP TABLE " + tableName);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));

        assertUpdate("DROP TABLE " + tableNameLike);
        assertFalse(getQueryRunner().tableExists(getSession(), tableNameLike));
    }

    @Test
    public void testCreateTableAsSelect()
    {
        String tableName = "test_ctas" + randomTableSuffix();
        if (!supportsCreateTable()) {
            assertQueryFails("CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name, regionkey FROM nation", "This connector does not support creating tables with data");
            return;
        }
        assertUpdate("CREATE TABLE IF NOT EXISTS " + tableName + " AS SELECT name, regionkey FROM nation", "SELECT count(*) FROM nation");
        assertTableColumnNames(tableName, "name", "regionkey");
        assertUpdate("DROP TABLE " + tableName);

        // Some connectors support CREATE TABLE AS but not the ordinary CREATE TABLE. Let's test CTAS IF NOT EXISTS with a table that is guaranteed to exist.
        assertUpdate("CREATE TABLE IF NOT EXISTS nation AS SELECT custkey, acctbal FROM customer", 0);
        assertTableColumnNames("nation", "nationkey", "name", "regionkey", "comment");

        assertCreateTableAsSelect(
                "SELECT custkey, address, acctbal FROM customer",
                "SELECT count(*) FROM customer");

        assertCreateTableAsSelect(
                "SELECT mktsegment, sum(acctbal) x FROM customer GROUP BY mktsegment",
                "SELECT count(DISTINCT mktsegment) FROM customer");

        assertCreateTableAsSelect(
                "SELECT count(*) x FROM customer JOIN nation ON customer.nationkey = nation.nationkey",
                "SELECT 1");

        assertCreateTableAsSelect(
                "SELECT custkey FROM customer ORDER BY custkey LIMIT 10",
                "SELECT 10");

        assertCreateTableAsSelect(
                "SELECT * FROM customer WITH DATA",
                "SELECT * FROM customer",
                "SELECT count(*) FROM customer");

        assertCreateTableAsSelect(
                "SELECT * FROM customer WITH NO DATA",
                "SELECT * FROM customer LIMIT 0",
                "SELECT 0");

        // Tests for CREATE TABLE with UNION ALL: exercises PushTableWriteThroughUnion optimizer

        assertCreateTableAsSelect(
                "SELECT name, custkey, acctbal FROM customer WHERE custkey % 2 = 0 UNION ALL " +
                        "SELECT name, custkey, acctbal FROM customer WHERE custkey % 2 = 1",
                "SELECT name, custkey, acctbal FROM customer",
                "SELECT count(*) FROM customer");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "true").build(),
                "SELECT CAST(custkey AS BIGINT) custkey, acctbal FROM customer UNION ALL " +
                        "SELECT 1234567890, 1.23",
                "SELECT custkey, acctbal FROM customer UNION ALL " +
                        "SELECT 1234567890, 1.23",
                "SELECT count(*) + 1 FROM customer");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "false").build(),
                "SELECT CAST(custkey AS BIGINT) custkey, acctbal FROM customer UNION ALL " +
                        "SELECT 1234567890, 1.23",
                "SELECT custkey, acctbal FROM customer UNION ALL " +
                        "SELECT 1234567890, 1.23",
                "SELECT count(*) + 1 FROM customer");

        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE " + tableName + " AS SELECT mktsegment FROM customer");
        assertQuery("SELECT * from " + tableName, "SELECT mktsegment FROM customer");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCreateTableAsSelectWithUnicode()
    {
        // Covered by testCreateTableAsSelect
        skipTestUnless(supportsCreateTable());
        assertCreateTableAsSelect(
                "SELECT '\u2603' unicode",
                "SELECT 1");
    }

    protected void assertCreateTableAsSelect(@Language("SQL") String query, @Language("SQL") String rowCountQuery)
    {
        assertCreateTableAsSelect(getSession(), query, query, rowCountQuery);
    }

    protected void assertCreateTableAsSelect(@Language("SQL") String query, @Language("SQL") String expectedQuery, @Language("SQL") String rowCountQuery)
    {
        assertCreateTableAsSelect(getSession(), query, expectedQuery, rowCountQuery);
    }

    protected void assertCreateTableAsSelect(Session session, @Language("SQL") String query, @Language("SQL") String expectedQuery, @Language("SQL") String rowCountQuery)
    {
        String table = "test_ctas_" + randomTableSuffix();
        assertUpdate(session, "CREATE TABLE " + table + " AS " + query, rowCountQuery);
        assertQuery(session, "SELECT * FROM " + table, expectedQuery);
        assertUpdate(session, "DROP TABLE " + table);

        assertFalse(getQueryRunner().tableExists(session, table));
    }

    @Test
    public void testRenameTable()
    {
        skipTestUnless(supportsCreateTable());
        String tableName = "test_rename_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 123 x", 1);

        String renamedTable = "test_rename_new_" + randomTableSuffix();
        if (!supportsRenameTable()) {
            assertQueryFails("ALTER TABLE " + tableName + " RENAME TO " + renamedTable, "This connector does not support renaming tables");
            return;
        }

        assertUpdate("ALTER TABLE " + tableName + " RENAME TO " + renamedTable);
        assertQuery("SELECT x FROM " + renamedTable, "VALUES 123");

        String testExistsTableName = "test_rename_exists_" + randomTableSuffix();
        assertUpdate("ALTER TABLE IF EXISTS " + renamedTable + " RENAME TO " + testExistsTableName);
        assertQuery("SELECT x FROM " + testExistsTableName, "VALUES 123");

        String uppercaseName = "TEST_RENAME_" + randomTableSuffix(); // Test an upper-case, not delimited identifier
        assertUpdate("ALTER TABLE " + testExistsTableName + " RENAME TO " + uppercaseName);
        assertQuery(
                "SELECT x FROM " + uppercaseName.toLowerCase(ENGLISH), // Ensure select allows for lower-case, not delimited identifier
                "VALUES 123");

        assertUpdate("DROP TABLE " + uppercaseName);

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertFalse(getQueryRunner().tableExists(getSession(), renamedTable));

        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " RENAME TO " + renamedTable);
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertFalse(getQueryRunner().tableExists(getSession(), renamedTable));
    }

    @Test
    public void testCommentTable()
    {
        if (!supportsCommentOnTable()) {
            assertQueryFails("COMMENT ON TABLE nation IS 'new comment'", "This connector does not support setting table comments");
            return;
        }

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_comment_", "(a integer)")) {
            // comment set
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS 'new comment'");
            assertThat((String) computeActual("SHOW CREATE TABLE " + table.getName()).getOnlyValue()).contains("COMMENT 'new comment'");
            assertThat(getTableComment(table.getName())).isEqualTo("new comment");
            assertThat(query(
                    "SELECT table_name, comment FROM system.metadata.table_comments " +
                            "WHERE catalog_name = '" + catalogName + "' AND " +
                            "schema_name = '" + schemaName + "'"))
                    .skippingTypesCheck()
                    .containsAll("VALUES ('" + table.getName() + "', 'new comment')");

            // comment updated
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS 'updated comment'");
            assertThat(getTableComment(table.getName())).isEqualTo("updated comment");

            // comment set to empty or deleted
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS ''");
            assertThat(getTableComment(table.getName())).isIn("", null); // Some storages do not preserve empty comment

            // comment deleted
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS 'a comment'");
            assertThat(getTableComment(table.getName())).isEqualTo("a comment");
            assertUpdate("COMMENT ON TABLE " + table.getName() + " IS NULL");
            assertThat(getTableComment(table.getName())).isEqualTo(null);
        }

        String tableName = "test_comment_" + randomTableSuffix();
        try {
            // comment set when creating a table
            assertUpdate("CREATE TABLE " + tableName + "(key integer) COMMENT 'new table comment'");
            assertThat(getTableComment(tableName)).isEqualTo("new table comment");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private String getTableComment(String tableName)
    {
        // TODO use information_schema.tables.table_comment
        String result = (String) computeActual("SHOW CREATE TABLE " + tableName).getOnlyValue();
        Matcher matcher = Pattern.compile("COMMENT '([^']*)'").matcher(result);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    @Test
    public void testCommentColumn()
    {
        if (!supportsCommentOnColumn()) {
            assertQueryFails("COMMENT ON COLUMN nation.nationkey IS 'new comment'", "This connector does not support setting column comments");
            return;
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_comment_column_", "(a integer)")) {
            // comment set
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS 'new comment'");
            assertThat((String) computeActual("SHOW CREATE TABLE " + table.getName()).getOnlyValue()).contains("COMMENT 'new comment'");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("new comment");

            // comment updated
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS 'updated comment'");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("updated comment");

            // comment set to empty or deleted
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS ''");
            assertThat(getColumnComment(table.getName(), "a")).isIn("", null); // Some storages do not preserve empty comment

            // comment deleted
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS 'a comment'");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo("a comment");
            assertUpdate("COMMENT ON COLUMN " + table.getName() + ".a IS NULL");
            assertThat(getColumnComment(table.getName(), "a")).isEqualTo(null);
        }

        // TODO: comment set when creating a table
//        assertUpdate("CREATE TABLE " + tableName + "(a integer COMMENT 'new column comment')");
//        assertThat(getColumnComment(tableName, "a")).isEqualTo("new column comment");
//        assertUpdate("DROP TABLE " + tableName);
    }

    private String getColumnComment(String tableName, String columnName)
    {
        MaterializedResult materializedResult = computeActual(format(
                "SELECT comment FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s'",
                getSession().getSchema().orElseThrow(),
                tableName,
                columnName));
        return (String) materializedResult.getOnlyValue();
    }

    @Test
    public void testRenameColumn()
    {
        skipTestUnless(supportsCreateTable());

        String tableName;
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_rename_column_", "AS SELECT 'some value' x")) {
            tableName = table.getName();
            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN x TO before_y");
            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN IF EXISTS before_y TO y");
            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN IF EXISTS columnNotExists TO y");
            assertQuery("SELECT y FROM " + tableName, "VALUES 'some value'");

            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN y TO Z"); // 'Z' is upper-case, not delimited
            assertQuery(
                    "SELECT z FROM " + tableName, // 'z' is lower-case, not delimited
                    "VALUES 'some value'");

            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN IF EXISTS z TO a");
            assertQuery(
                    "SELECT a FROM " + tableName,
                    "VALUES 'some value'");

            // There should be exactly one column
            assertQuery("SELECT * FROM " + tableName, "VALUES 'some value'");
        }

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " RENAME COLUMN columnNotExists TO y");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " RENAME COLUMN IF EXISTS columnNotExists TO y");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testDropColumn()
    {
        skipTestUnless(supportsCreateTable());

        String tableName;
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_drop_column_", "AS SELECT 123 x, 456 y, 111 a")) {
            tableName = table.getName();
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN x");
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS y");
            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
            assertQueryFails("SELECT x FROM " + tableName, ".* Column 'x' cannot be resolved");
            assertQueryFails("SELECT y FROM " + tableName, ".* Column 'y' cannot be resolved");

            assertQueryFails("ALTER TABLE " + tableName + " DROP COLUMN a", ".* Cannot drop the only column in a table");
        }

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN notExistColumn");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " DROP COLUMN IF EXISTS notExistColumn");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testAddColumn()
    {
        skipTestUnless(supportsCreateTable());

        String tableName;
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_add_column_", "AS SELECT VARCHAR 'first' x")) {
            tableName = table.getName();
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN x bigint", ".* Column 'x' already exists");
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN X bigint", ".* Column 'X' already exists");
            assertQueryFails("ALTER TABLE " + table.getName() + " ADD COLUMN q bad_type", ".* Unknown type 'bad_type' for column 'q'");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN a varchar(50)");
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'second', 'xxx'", 1);
            assertQuery(
                    "SELECT x, a FROM " + table.getName(),
                    "VALUES ('first', NULL), ('second', 'xxx')");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN b double");
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'third', 'yyy', 33.3E0", 1);
            assertQuery(
                    "SELECT x, a, b FROM " + table.getName(),
                    "VALUES ('first', NULL, NULL), ('second', 'xxx', NULL), ('third', 'yyy', 33.3)");

            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN IF NOT EXISTS c varchar(50)");
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN IF NOT EXISTS c varchar(50)");
            assertUpdate("INSERT INTO " + table.getName() + " SELECT 'fourth', 'zzz', 55.3E0, 'newColumn'", 1);
            assertQuery(
                    "SELECT x, a, b, c FROM " + table.getName(),
                    "VALUES ('first', NULL, NULL, NULL), ('second', 'xxx', NULL, NULL), ('third', 'yyy', 33.3, NULL), ('fourth', 'zzz', 55.3, 'newColumn')");
        }

        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN x bigint");
        assertUpdate("ALTER TABLE IF EXISTS " + tableName + " ADD COLUMN IF NOT EXISTS x bigint");
        assertFalse(getQueryRunner().tableExists(getSession(), tableName));
    }

    @Test
    public void testInsert()
    {
        if (!supportsInsert()) {
            assertQueryFails("INSERT INTO nation(nationkey) VALUES (42)", "This connector does not support inserts");
            return;
        }

        String query = "SELECT phone, custkey, acctbal FROM customer";

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_", "AS " + query + " WITH NO DATA")) {
            assertQuery("SELECT count(*) FROM " + table.getName() + "", "SELECT 0");

            assertUpdate("INSERT INTO " + table.getName() + " " + query, "SELECT count(*) FROM customer");

            assertQuery("SELECT * FROM " + table.getName() + "", query);

            assertUpdate("INSERT INTO " + table.getName() + " (custkey) VALUES (-1)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (custkey) VALUES (null)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (phone) VALUES ('3283-2001-01-01')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (custkey, phone) VALUES (-2, '3283-2001-01-02')", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (phone, custkey) VALUES ('3283-2001-01-03', -3)", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (acctbal) VALUES (1234)", 1);

            assertQuery("SELECT * FROM " + table.getName() + "", query
                    + " UNION ALL SELECT null, -1, null"
                    + " UNION ALL SELECT null, null, null"
                    + " UNION ALL SELECT '3283-2001-01-01', null, null"
                    + " UNION ALL SELECT '3283-2001-01-02', -2, null"
                    + " UNION ALL SELECT '3283-2001-01-03', -3, null"
                    + " UNION ALL SELECT null, null, 1234");

            // UNION query produces columns in the opposite order
            // of how they are declared in the table schema
            assertUpdate(
                    "INSERT INTO " + table.getName() + " (custkey, phone, acctbal) " +
                            "SELECT custkey, phone, acctbal FROM customer " +
                            "UNION ALL " +
                            "SELECT custkey, phone, acctbal FROM customer",
                    "SELECT 2 * count(*) FROM customer");
        }
    }

    @Test
    public void testInsertUnicode()
    {
        skipTestUnless(supportsInsert());

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_unicode_", "(test varchar(50))")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5world\\7F16\\7801' ", 2);
            assertThat(computeActual("SELECT test FROM " + table.getName()).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("Hello", "hello测试world编码");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_unicode_", "(test varchar(50))")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'aa', 'bé'", 2);
            assertQuery("SELECT test FROM " + table.getName(), "VALUES 'aa', 'bé'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test = 'aa'", "VALUES 'aa'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test > 'ba'", "VALUES 'bé'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test < 'ba'", "VALUES 'aa'");
            assertQueryReturnsEmptyResult("SELECT test FROM " + table.getName() + " WHERE test = 'ba'");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_unicode_", "(test varchar(50))")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'a', 'é'", 2);
            assertQuery("SELECT test FROM " + table.getName(), "VALUES 'a', 'é'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test = 'a'", "VALUES 'a'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test > 'b'", "VALUES 'é'");
            assertQuery("SELECT test FROM " + table.getName() + " WHERE test < 'b'", "VALUES 'a'");
            assertQueryReturnsEmptyResult("SELECT test FROM " + table.getName() + " WHERE test = 'b'");
        }
    }

    @Test
    public void testInsertHighestUnicodeCharacter()
    {
        skipTestUnless(supportsInsert());

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_unicode_", "(test varchar(50))")) {
            assertUpdate("INSERT INTO " + table.getName() + "(test) VALUES 'Hello', U&'hello\\6d4B\\8Bd5\\+10FFFFworld\\7F16\\7801' ", 2);
            assertThat(computeActual("SELECT test FROM " + table.getName()).getOnlyColumnAsSet())
                    .containsExactlyInAnyOrder("Hello", "hello测试􏿿world编码");
        }
    }

    @Test
    public void testInsertArray()
    {
        skipTestUnless(supportsInsert());

        String tableName = "test_insert_array_" + randomTableSuffix();
        if (!supportsArrays()) {
            assertThatThrownBy(() -> query("CREATE TABLE " + tableName + " (a array(bigint))"))
                    // TODO Unify failure message across connectors
                    .hasMessageMatching("[Uu]nsupported (column )?type: \\Qarray(bigint)");
            throw new SkipException("not supported");
        }

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_insert_array_", "(a ARRAY<DOUBLE>, b ARRAY<BIGINT>)")) {
            assertUpdate("INSERT INTO " + table.getName() + " (a) VALUES (ARRAY[null])", 1);
            assertUpdate("INSERT INTO " + table.getName() + " (a, b) VALUES (ARRAY[1.23E1], ARRAY[1.23E1])", 1);
            assertQuery("SELECT a[1], b[1] FROM " + table.getName(), "VALUES (null, null), (12.3, 12)");
        }
    }

    @Test
    public void testDelete()
    {
        skipTestUnlessSupportsDeletes();

        // delete successive parts of the table
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_", "AS SELECT * FROM orders")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE custkey <= 100", "SELECT count(*) FROM orders WHERE custkey <= 100");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM orders WHERE custkey > 100");

            assertUpdate("DELETE FROM " + table.getName() + " WHERE custkey <= 300", "SELECT count(*) FROM orders WHERE custkey > 100 AND custkey <= 300");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM orders WHERE custkey > 300");

            assertUpdate("DELETE FROM " + table.getName() + " WHERE custkey <= 500", "SELECT count(*) FROM orders WHERE custkey > 300 AND custkey <= 500");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM orders WHERE custkey > 500");
        }

        // delete without matching any rows
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_", "AS SELECT * FROM orders")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE orderkey < 0", 0);
        }

        // delete with a predicate that optimizes to false
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_", "AS SELECT * FROM orders")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE orderkey > 5 AND orderkey < 4", 0);
        }

        String tableName = "test_delete_" + randomTableSuffix();
        try {
            // test EXPLAIN ANALYZE with CTAS
            assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE " + tableName + " AS SELECT CAST(orderstatus AS VARCHAR(15)) orderstatus FROM orders");
            assertQuery("SELECT * from " + tableName, "SELECT orderstatus FROM orders");
            // check that INSERT works also
            assertExplainAnalyze("EXPLAIN ANALYZE INSERT INTO " + tableName + " SELECT clerk FROM orders");
            assertQuery("SELECT * from " + tableName, "SELECT orderstatus FROM orders UNION ALL SELECT clerk FROM orders");
            // check DELETE works with EXPLAIN ANALYZE
            assertExplainAnalyze("EXPLAIN ANALYZE DELETE FROM " + tableName + " WHERE TRUE");
            assertQuery("SELECT COUNT(*) from " + tableName, "SELECT 0");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testDeleteWithComplexPredicate()
    {
        skipTestUnlessSupportsDeletes();

        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_complex_", "AS SELECT * FROM orders")) {
            // delete half the table, then delete the rest
            assertUpdate("DELETE FROM " + table.getName() + " WHERE orderkey % 2 = 0", "SELECT count(*) FROM orders WHERE orderkey % 2 = 0");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM orders WHERE orderkey % 2 <> 0");

            assertUpdate("DELETE FROM " + table.getName(), "SELECT count(*) FROM orders WHERE orderkey % 2 <> 0");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM orders LIMIT 0");

            assertUpdate("DELETE FROM " + table.getName() + " WHERE rand() < 0", 0);
        }
    }

    @Test
    public void testDeleteWithSubquery()
    {
        skipTestUnlessSupportsDeletes();

        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_subquery", "AS SELECT * FROM nation")) {
            // delete using a subquery
            assertUpdate("DELETE FROM " + table.getName() + " WHERE regionkey IN (SELECT regionkey FROM region WHERE name LIKE 'A%')", 15);
            assertQuery(
                    "SELECT * FROM " + table.getName(),
                    "SELECT * FROM nation WHERE regionkey IN (SELECT regionkey FROM region WHERE name NOT LIKE 'A%')");
        }

        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_subquery", "AS SELECT * FROM orders")) {
            // delete using a scalar and EXISTS subquery
            assertUpdate("DELETE FROM " + table.getName() + " WHERE orderkey = (SELECT orderkey FROM orders ORDER BY orderkey LIMIT 1)", 1);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE orderkey = (SELECT orderkey FROM orders WHERE false)", 0);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE EXISTS(SELECT 1 WHERE false)", 0);
            assertUpdate("DELETE FROM " + table.getName() + " WHERE EXISTS(SELECT 1)", "SELECT count(*) - 1 FROM orders");
        }
    }

    @Test
    public void testExplainAnalyzeWithDeleteWithSubquery()
    {
        skipTestUnlessSupportsDeletes();

        String tableName = "test_delete_" + randomTableSuffix();

        // delete using a subquery
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM nation", 25);
        assertExplainAnalyze("EXPLAIN ANALYZE DELETE FROM " + tableName + " WHERE regionkey IN (SELECT regionkey FROM region WHERE name LIKE 'A%' LIMIT 1)",
                "SemiJoin.*");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDeleteWithSemiJoin()
    {
        skipTestUnlessSupportsDeletes();

        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_semijoin", "AS SELECT * FROM nation")) {
            // delete with multiple SemiJoin
            assertUpdate(
                    "DELETE FROM " + table.getName() + " " +
                            "WHERE regionkey IN (SELECT regionkey FROM region WHERE name LIKE 'A%') " +
                            "  AND regionkey IN (SELECT regionkey FROM region WHERE length(comment) < 50)",
                    10);
            assertQuery(
                    "SELECT * FROM " + table.getName(),
                    "SELECT * FROM nation " +
                            "WHERE regionkey IN (SELECT regionkey FROM region WHERE name NOT LIKE 'A%') " +
                            "  OR regionkey IN (SELECT regionkey FROM region WHERE length(comment) >= 50)");
        }

        // TODO (https://github.com/trinodb/trino/issues/5901) Use longer table name once Oracle version is updated
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_semijoin", "AS SELECT * FROM orders")) {
            // delete with SemiJoin null handling
            assertUpdate(
                    "DELETE FROM " + table.getName() + "\n" +
                            "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM tpch.tiny.lineitem)) IS NULL\n",
                    "SELECT count(*) FROM orders\n" +
                            "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM lineitem)) IS NULL\n");
            assertQuery(
                    "SELECT * FROM " + table.getName(),
                    "SELECT * FROM orders\n" +
                            "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM lineitem)) IS NOT NULL\n");
        }
    }

    @Test
    public void testDeleteWithVarcharPredicate()
    {
        skipTestUnlessSupportsDeletes();

        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_delete_with_varchar_predicate_", "AS SELECT * FROM orders")) {
            assertUpdate("DELETE FROM " + table.getName() + " WHERE orderstatus = 'O'", "SELECT count(*) FROM orders WHERE orderstatus = 'O'");
            assertQuery("SELECT * FROM " + table.getName(), "SELECT * FROM orders WHERE orderstatus <> 'O'");
        }
    }

    protected void skipTestUnlessSupportsDeletes()
    {
        skipTestUnless(supportsCreateTable());
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_supports_delete", "(col varchar(1))")) {
            if (!supportsDelete()) {
                assertQueryFails("DELETE FROM " + table.getName(), "This connector does not support deletes");
                throw new SkipException("This connector does not support deletes");
            }
        }
    }

    @Test
    public void testDropTableIfExists()
    {
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop_if_exists"));
        assertUpdate("DROP TABLE IF EXISTS test_drop_if_exists");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop_if_exists"));
    }

    @Test
    public void testView()
    {
        if (!supportsViews()) {
            assertQueryFails("CREATE VIEW nation_v AS SELECT * FROM nation", "This connector does not support creating views");
            return;
        }

        @Language("SQL") String query = "SELECT orderkey, orderstatus, (totalprice / 2) half FROM orders";

        String catalogName = getSession().getCatalog().orElseThrow();
        String schemaName = getSession().getSchema().orElseThrow();
        String testView = "test_view_" + randomTableSuffix();
        String testViewWithComment = "test_view_with_comment_" + randomTableSuffix();
        assertUpdate("CREATE VIEW " + testView + " AS SELECT 123 x");
        assertUpdate("CREATE OR REPLACE VIEW " + testView + " AS " + query);

        assertUpdate("CREATE VIEW " + testViewWithComment + " COMMENT 'orders' AS SELECT 123 x");
        assertUpdate("CREATE OR REPLACE VIEW " + testViewWithComment + " COMMENT 'orders' AS " + query);

        // verify comment
        MaterializedResult materializedRows = computeActual("SHOW CREATE VIEW " + testViewWithComment);
        assertThat((String) materializedRows.getOnlyValue()).contains("COMMENT 'orders'");
        assertThat(query(
                "SELECT table_name, comment FROM system.metadata.table_comments " +
                        "WHERE catalog_name = '" + catalogName + "' AND " +
                        "schema_name = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + testView + "', null), ('" + testViewWithComment + "', 'orders')");

        // reading
        assertQuery("SELECT * FROM " + testView, query);
        assertQuery("SELECT * FROM " + testViewWithComment, query);

        assertQuery(
                "SELECT * FROM " + testView + " a JOIN " + testView + " b on a.orderkey = b.orderkey",
                format("SELECT * FROM (%s) a JOIN (%s) b ON a.orderkey = b.orderkey", query, query));

        assertQuery("WITH orders AS (SELECT * FROM orders LIMIT 0) SELECT * FROM " + testView, query);

        String name = format("%s.%s." + testView, catalogName, schemaName);
        assertQuery("SELECT * FROM " + name, query);

        assertUpdate("DROP VIEW " + testViewWithComment);

        // information_schema.views without table_name filter
        assertThat(query(
                "SELECT table_name, regexp_replace(view_definition, '\\s', '') FROM information_schema.views " +
                        "WHERE table_schema = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + testView + "', '" + query.replaceAll("\\s", "") + "')");
        // information_schema.views with table_name filter
        assertQuery(
                "SELECT table_name, regexp_replace(view_definition, '\\s', '') FROM information_schema.views " +
                        "WHERE table_schema = '" + schemaName + "' and table_name = '" + testView + "'",
                "VALUES ('" + testView + "', '" + query.replaceAll("\\s", "") + "')");

        // table listing
        assertThat(query("SHOW TABLES"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + testView + "'");
        // information_schema.tables without table_name filter
        assertThat(query(
                "SELECT table_name, table_type FROM information_schema.tables " +
                        "WHERE table_schema = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + testView + "', 'VIEW')");
        // information_schema.tables with table_name filter
        assertQuery(
                "SELECT table_name, table_type FROM information_schema.tables " +
                        "WHERE table_schema = '" + schemaName + "' and table_name = '" + testView + "'",
                "VALUES ('" + testView + "', 'VIEW')");

        // system.jdbc.tables without filter
        assertThat(query("SELECT table_schem, table_name, table_type FROM system.jdbc.tables"))
                .skippingTypesCheck()
                .containsAll("VALUES ('" + schemaName + "', '" + testView + "', 'VIEW')");

        // system.jdbc.tables with table prefix filter
        assertQuery(
                "SELECT table_schem, table_name, table_type " +
                        "FROM system.jdbc.tables " +
                        "WHERE table_cat = '" + catalogName + "' AND " +
                        "table_schem = '" + schemaName + "' AND " +
                        "table_name = '" + testView + "'",
                "VALUES ('" + schemaName + "', '" + testView + "', 'VIEW')");

        // column listing
        assertThat(query("SHOW COLUMNS FROM " + testView))
                .projected(0) // column types can very between connectors
                .skippingTypesCheck()
                .matches("VALUES 'orderkey', 'orderstatus', 'half'");

        assertThat(query("DESCRIBE " + testView))
                .projected(0) // column types can very between connectors
                .skippingTypesCheck()
                .matches("VALUES 'orderkey', 'orderstatus', 'half'");

        // information_schema.columns without table_name filter
        assertThat(query(
                "SELECT table_name, column_name " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES '" + testView + "') " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])");

        // information_schema.columns with table_name filter
        assertThat(query(
                "SELECT table_name, column_name " +
                        "FROM information_schema.columns " +
                        "WHERE table_schema = '" + schemaName + "' and table_name = '" + testView + "'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES '" + testView + "') " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])");

        // view-specific listings
        assertThat(query("SELECT table_name FROM information_schema.views WHERE table_schema = '" + schemaName + "'"))
                .skippingTypesCheck()
                .containsAll("VALUES '" + testView + "'");

        // system.jdbc.columns without filter
        assertThat(query("SELECT table_schem, table_name, column_name FROM system.jdbc.columns"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + schemaName + "', '" + testView + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])");

        // system.jdbc.columns with schema filter
        assertThat(query(
                "SELECT table_schem, table_name, column_name " +
                        "FROM system.jdbc.columns " +
                        "WHERE table_schem LIKE '%" + schemaName + "%'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + schemaName + "', '" + testView + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])");

        // system.jdbc.columns with table filter
        assertThat(query(
                "SELECT table_schem, table_name, column_name " +
                        "FROM system.jdbc.columns " +
                        "WHERE table_name LIKE '%" + testView + "%'"))
                .skippingTypesCheck()
                .containsAll(
                        "SELECT * FROM (VALUES ('" + schemaName + "', '" + testView + "')) " +
                                "CROSS JOIN UNNEST(ARRAY['orderkey', 'orderstatus', 'half'])");

        assertUpdate("DROP VIEW " + testView);
    }

    @Test
    public void testViewCaseSensitivity()
    {
        skipTestUnless(supportsViews());

        String upperCaseView = "test_view_uppercase_" + randomTableSuffix();
        String mixedCaseView = "test_view_mixedcase_" + randomTableSuffix();

        computeActual("CREATE VIEW " + upperCaseView + " AS SELECT X FROM (SELECT 123 X)");
        computeActual("CREATE VIEW " + mixedCaseView + " AS SELECT XyZ FROM (SELECT 456 XyZ)");
        assertQuery("SELECT * FROM " + upperCaseView, "SELECT X FROM (SELECT 123 X)");
        assertQuery("SELECT * FROM " + mixedCaseView, "SELECT XyZ FROM (SELECT 456 XyZ)");

        assertUpdate("DROP VIEW " + upperCaseView);
        assertUpdate("DROP VIEW " + mixedCaseView);
    }

    @Test
    public void testCompatibleTypeChangeForView()
    {
        skipTestUnless(supportsViews());

        String tableName = "test_table_" + randomTableSuffix();
        String viewName = "test_view_" + randomTableSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 'abcdefg' a", 1);
        assertUpdate("CREATE VIEW " + viewName + " AS SELECT a FROM " + tableName);

        assertQuery("SELECT * FROM " + viewName, "VALUES 'abcdefg'");

        // replace table with a version that's implicitly coercible to the previous one
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 'abc' a", 1);

        assertQuery("SELECT * FROM " + viewName, "VALUES 'abc'");

        assertUpdate("DROP VIEW " + viewName);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testCompatibleTypeChangeForView2()
    {
        skipTestUnless(supportsViews());

        String tableName = "test_table_" + randomTableSuffix();
        String viewName = "test_view_" + randomTableSuffix();

        assertUpdate("CREATE TABLE " + tableName + " AS SELECT BIGINT '1' v", 1);
        assertUpdate("CREATE VIEW " + viewName + " AS SELECT * FROM " + tableName);

        assertQuery("SELECT * FROM " + viewName, "VALUES 1");

        // replace table with a version that's implicitly coercible to the previous one
        assertUpdate("DROP TABLE " + tableName);
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT INTEGER '1' v", 1);

        assertQuery("SELECT * FROM " + viewName + " WHERE v = 1", "VALUES 1");

        assertUpdate("DROP VIEW " + viewName);
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test(dataProvider = "testViewMetadataDataProvider")
    public void testViewMetadata(String securityClauseInCreate, String securityClauseInShowCreate)
    {
        skipTestUnless(supportsViews());

        String viewName = "meta_test_view_" + randomTableSuffix();

        @Language("SQL") String query = "SELECT BIGINT '123' x, 'foo' y";
        assertUpdate("CREATE VIEW " + viewName + securityClauseInCreate + " AS " + query);

        // test INFORMATION_SCHEMA.TABLES
        MaterializedResult actual = computeActual(format(
                "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = '%s'",
                getSession().getSchema().get()));

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("customer", "BASE TABLE")
                .row(viewName, "VIEW")
                .row("nation", "BASE TABLE")
                .row("orders", "BASE TABLE")
                .row("region", "BASE TABLE")
                .build();

        assertContains(actual, expected);

        // test SHOW TABLES
        actual = computeActual("SHOW TABLES");

        MaterializedResult.Builder builder = resultBuilder(getSession(), actual.getTypes());
        for (MaterializedRow row : expected.getMaterializedRows()) {
            builder.row(row.getField(0));
        }
        expected = builder.build();

        assertContains(actual, expected);

        // test INFORMATION_SCHEMA.VIEWS
        actual = computeActual(format(
                "SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = '%s'",
                getSession().getSchema().get()));

        expected = resultBuilder(getSession(), actual.getTypes())
                .row(viewName, formatSqlText(query))
                .build();

        assertContains(actual, expected);

        // test SHOW COLUMNS
        actual = computeActual("SHOW COLUMNS FROM " + viewName);

        expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("x", "bigint", "", "")
                .row("y", "varchar(3)", "", "")
                .build();

        assertEquals(actual, expected);

        // test SHOW CREATE VIEW
        String expectedSql = formatSqlText(format(
                "CREATE VIEW %s.%s.%s SECURITY %s AS %s",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                viewName,
                securityClauseInShowCreate,
                query)).trim();

        actual = computeActual("SHOW CREATE VIEW " + viewName);

        assertEquals(getOnlyElement(actual.getOnlyColumnAsSet()), expectedSql);

        actual = computeActual(format("SHOW CREATE VIEW %s.%s.%s", getSession().getCatalog().get(), getSession().getSchema().get(), viewName));

        assertEquals(getOnlyElement(actual.getOnlyColumnAsSet()), expectedSql);

        assertUpdate("DROP VIEW " + viewName);
    }

    @DataProvider
    public static Object[][] testViewMetadataDataProvider()
    {
        return new Object[][] {
                {"", "DEFINER"},
                {" SECURITY DEFINER", "DEFINER"},
                {" SECURITY INVOKER", "INVOKER"},
        };
    }

    @Test
    public void testShowCreateView()
    {
        skipTestUnless(supportsViews());
        checkState(getSession().getCatalog().isPresent(), "catalog is not set");
        checkState(getSession().getSchema().isPresent(), "schema is not set");

        String viewName = "test_show_create_view" + randomTableSuffix();
        assertUpdate("DROP VIEW IF EXISTS " + viewName);
        String ddl = format(
                "CREATE VIEW %s.%s.%s SECURITY DEFINER AS\n" +
                        "SELECT *\n" +
                        "FROM\n" +
                        "  (\n" +
                        " VALUES \n" +
                        "     ROW (1, 'one')\n" +
                        "   , ROW (2, 't')\n" +
                        ")  t (col1, col2)",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                viewName);
        assertUpdate(ddl);

        assertEquals(computeActual("SHOW CREATE VIEW " + viewName).getOnlyValue(), ddl);

        assertUpdate("DROP VIEW " + viewName);
    }

    @Test
    public void testQueryLoggingCount()
    {
        skipTestUnless(supportsCreateTable());

        QueryManager queryManager = getDistributedQueryRunner().getCoordinator().getQueryManager();
        executeExclusively(() -> {
            assertEventually(
                    new Duration(1, MINUTES),
                    () -> assertEquals(
                            queryManager.getQueries().stream()
                                    .map(BasicQueryInfo::getQueryId)
                                    .map(queryManager::getFullQueryInfo)
                                    .filter(info -> !info.isFinalQueryInfo())
                                    .collect(toList()),
                            ImmutableList.of()));

            // We cannot simply get the number of completed queries as soon as all the queries are completed, because this counter may not be up-to-date at that point.
            // The completed queries counter is updated in a final query info listener, which is called eventually.
            // Therefore, here we wait until the value of this counter gets stable.

            DispatchManager dispatchManager = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getDispatchManager();
            long beforeCompletedQueriesCount = waitUntilStable(() -> dispatchManager.getStats().getCompletedQueries().getTotalCount(), new Duration(5, SECONDS));
            long beforeSubmittedQueriesCount = dispatchManager.getStats().getSubmittedQueries().getTotalCount();
            String tableName = "test_logging_count" + randomTableSuffix();
            assertUpdate("CREATE TABLE " + tableName + tableDefinitionForQueryLoggingCount());
            assertQueryReturnsEmptyResult("SELECT foo_1, foo_2_4 FROM " + tableName);
            assertUpdate("DROP TABLE " + tableName);
            assertQueryFails("SELECT * FROM " + tableName, ".*Table .* does not exist");

            // TODO: Figure out a better way of synchronization
            assertEventually(
                    new Duration(1, MINUTES),
                    () -> assertEquals(dispatchManager.getStats().getCompletedQueries().getTotalCount() - beforeCompletedQueriesCount, 4));
            assertEquals(dispatchManager.getStats().getSubmittedQueries().getTotalCount() - beforeSubmittedQueriesCount, 4);
        });
    }

    /**
     * The table must have two columns foo_1 and foo_2_4 of any type.
     */
    @Language("SQL")
    protected String tableDefinitionForQueryLoggingCount()
    {
        return "(foo_1 int, foo_2_4 int)";
    }

    private <T> T waitUntilStable(Supplier<T> computation, Duration timeout)
    {
        T lastValue = computation.get();
        long start = System.nanoTime();
        while (!currentThread().isInterrupted() && nanosSince(start).compareTo(timeout) < 0) {
            sleepUninterruptibly(100, MILLISECONDS);
            T currentValue = computation.get();
            if (currentValue.equals(lastValue)) {
                return currentValue;
            }
            lastValue = currentValue;
        }
        throw new UncheckedTimeoutException();
    }

    @Test
    public void testShowSchemasFromOther()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS FROM tpch");
        assertTrue(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of(INFORMATION_SCHEMA, "tiny", "sf1")));
    }

    // TODO move to to engine-only
    @Test
    public void testSymbolAliasing()
    {
        skipTestUnless(supportsCreateTable());

        String tableName = "test_symbol_aliasing" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT 1 foo_1, 2 foo_2_4", 1);
        assertQuery("SELECT foo_1, foo_2_4 FROM " + tableName, "SELECT 1, 2");
        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testWrittenStats()
    {
        skipTestUnless(supportsCreateTable());
        skipTestUnless(supportsInsert());

        String tableName = "test_written_stats_" + randomTableSuffix();
        try {
            String sql = "CREATE TABLE " + tableName + " AS SELECT * FROM nation";
            ResultWithQueryId<MaterializedResult> resultResultWithQueryId = getDistributedQueryRunner().executeWithQueryId(getSession(), sql);
            QueryInfo queryInfo = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());

            assertEquals(queryInfo.getQueryStats().getOutputPositions(), 1L);
            assertEquals(queryInfo.getQueryStats().getWrittenPositions(), 25L);
            assertTrue(queryInfo.getQueryStats().getLogicalWrittenDataSize().toBytes() > 0L);

            sql = "INSERT INTO " + tableName + " SELECT * FROM nation LIMIT 10";
            resultResultWithQueryId = getDistributedQueryRunner().executeWithQueryId(getSession(), sql);
            queryInfo = getDistributedQueryRunner().getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());

            assertEquals(queryInfo.getQueryStats().getOutputPositions(), 1L);
            assertEquals(queryInfo.getQueryStats().getWrittenPositions(), 10L);
            assertTrue(queryInfo.getQueryStats().getLogicalWrittenDataSize().toBytes() > 0L);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testShowCreateSchema()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE SCHEMA " + schemaName))
                .isEqualTo(format("CREATE SCHEMA %s.%s", getSession().getCatalog().orElseThrow(), schemaName));
    }

    @Test
    public void testCreateSchema()
    {
        String schemaName = "test_schema_create_" + randomTableSuffix();
        if (!supportsCreateSchema()) {
            assertQueryFails("CREATE SCHEMA " + schemaName, "This connector does not support creating schemas");
            return;
        }
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain(schemaName);
        assertUpdate("CREATE SCHEMA " + schemaName);

        // verify listing of new schema
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).contains(schemaName);

        // verify SHOW CREATE SCHEMA works
        assertThat((String) computeScalar("SHOW CREATE SCHEMA " + schemaName))
                .startsWith(format("CREATE SCHEMA %s.%s", getSession().getCatalog().orElseThrow(), schemaName));

        // try to create duplicate schema
        assertQueryFails("CREATE SCHEMA " + schemaName, format("line 1:1: Schema '.*\\.%s' already exists", schemaName));

        // cleanup
        assertUpdate("DROP SCHEMA " + schemaName);

        // verify DROP SCHEMA for non-existing schema
        assertQueryFails("DROP SCHEMA " + schemaName, format("line 1:1: Schema '.*\\.%s' does not exist", schemaName));
    }

    @Test
    public void testDropNonEmptySchema()
    {
        String schemaName = "test_drop_non_empty_schema_" + randomTableSuffix();
        if (!supportsDropSchema()) {
            return;
        }

        try {
            assertUpdate("CREATE SCHEMA " + schemaName);
            assertUpdate("CREATE TABLE " + schemaName + ".t(x int)");
            assertQueryFails("DROP SCHEMA " + schemaName, ".*Cannot drop non-empty schema '\\Q" + schemaName + "\\E'");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + schemaName + ".t");
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
        }
    }

    @Test
    public void testInsertForDefaultColumn()
    {
        skipTestUnless(supportsInsert());

        try (TestTable testTable = createTableWithDefaultColumns()) {
            assertUpdate(format("INSERT INTO %s (col_required, col_required2) VALUES (1, 10)", testTable.getName()), 1);
            assertUpdate(format("INSERT INTO %s VALUES (2, 3, 4, 5, 6)", testTable.getName()), 1);
            assertUpdate(format("INSERT INTO %s VALUES (7, null, null, 8, 9)", testTable.getName()), 1);
            assertUpdate(format("INSERT INTO %s (col_required2, col_required) VALUES (12, 13)", testTable.getName()), 1);

            assertQuery("SELECT * FROM " + testTable.getName(), "VALUES (1, null, 43, 42, 10), (2, 3, 4, 5, 6), (7, null, null, 8, 9), (13, null, 43, 42, 12)");
        }
    }

    protected TestTable createTableWithDefaultColumns()
    {
        throw new UnsupportedOperationException();
    }

    @Test(dataProvider = "testColumnNameDataProvider")
    public void testColumnName(String columnName)
    {
        skipTestUnless(supportsCreateTable());

        if (!requiresDelimiting(columnName)) {
            testColumnName(columnName, false);
        }
        testColumnName(columnName, true);
    }

    protected void testColumnName(String columnName, boolean delimited)
    {
        String nameInSql = columnName;
        if (delimited) {
            nameInSql = "\"" + columnName.replace("\"", "\"\"") + "\"";
        }
        String tableName = "tcn_" + nameInSql.toLowerCase(ENGLISH).replaceAll("[^a-z0-9]", "") + randomTableSuffix();

        try {
            // TODO test with both CTAS *and* CREATE TABLE + INSERT, since they use different connector API methods.
            assertUpdate("CREATE TABLE " + tableName + "(key varchar(50), " + nameInSql + " varchar(50))");
        }
        catch (RuntimeException e) {
            if (isColumnNameRejected(e, columnName, delimited)) {
                // It is OK if give column name is not allowed and is clearly rejected by the connector.
                return;
            }
            throw e;
        }
        try {
            assertUpdate("INSERT INTO " + tableName + " VALUES ('null value', NULL), ('sample value', 'abc'), ('other value', 'xyz')", 3);

            // SELECT *
            assertQuery("SELECT * FROM " + tableName, "VALUES ('null value', NULL), ('sample value', 'abc'), ('other value', 'xyz')");

            // projection
            assertQuery("SELECT " + nameInSql + " FROM " + tableName, "VALUES (NULL), ('abc'), ('xyz')");

            // predicate
            assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " IS NULL", "VALUES ('null value')");
            assertQuery("SELECT key FROM " + tableName + " WHERE " + nameInSql + " = 'abc'", "VALUES ('sample value')");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    protected boolean isColumnNameRejected(Exception exception, String columnName, boolean delimited)
    {
        return false;
    }

    protected static boolean requiresDelimiting(String identifierName)
    {
        return !identifierName.matches("[a-zA-Z][a-zA-Z0-9_]*");
    }

    @DataProvider
    public Object[][] testColumnNameDataProvider()
    {
        return testColumnNameTestData().stream()
                .map(this::filterColumnNameTestData)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toDataProvider());
    }

    private List<String> testColumnNameTestData()
    {
        return ImmutableList.<String>builder()
                .add("lowercase")
                .add("UPPERCASE")
                .add("MixedCase")
                .add("an_underscore")
                .add("a-hyphen-minus") // ASCII '-' is HYPHEN-MINUS in Unicode
                .add("a space")
                .add("atrailingspace ")
                .add(" aleadingspace")
                .add("a.dot")
                .add("a,comma")
                .add("a:colon")
                .add("a;semicolon")
                .add("an@at")
                .add("a\"quote")
                .add("an'apostrophe")
                .add("a`backtick`")
                .add("a/slash`")
                .add("a\\backslash`")
                .add("adigit0")
                .add("0startwithdigit")
                .build();
    }

    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        return Optional.of(columnName);
    }

    protected String dataMappingTableName(String trinoTypeName)
    {
        return "test_data_mapping_smoke_" + trinoTypeName.replaceAll("[^a-zA-Z0-9]", "_") + "_" + randomTableSuffix();
    }

    @Test(dataProvider = "testDataMappingSmokeTestDataProvider")
    public void testDataMappingSmokeTest(DataMappingTestSetup dataMappingTestSetup)
    {
        testDataMapping(dataMappingTestSetup);
    }

    private void testDataMapping(DataMappingTestSetup dataMappingTestSetup)
    {
        skipTestUnless(supportsCreateTable());

        String trinoTypeName = dataMappingTestSetup.getTrinoTypeName();
        String sampleValueLiteral = dataMappingTestSetup.getSampleValueLiteral();
        String highValueLiteral = dataMappingTestSetup.getHighValueLiteral();

        String tableName = dataMappingTableName(trinoTypeName);

        Runnable setup = () -> {
            // TODO test with both CTAS *and* CREATE TABLE + INSERT, since they use different connector API methods.
            String createTable = "" +
                    "CREATE TABLE " + tableName + " AS " +
                    "SELECT CAST(row_id AS varchar(50)) row_id, CAST(value AS " + trinoTypeName + ") value " +
                    "FROM (VALUES " +
                    "  ('null value', NULL), " +
                    "  ('sample value', " + sampleValueLiteral + "), " +
                    "  ('high value', " + highValueLiteral + ")) " +
                    " t(row_id, value)";
            assertUpdate(createTable, 3);
        };
        if (dataMappingTestSetup.isUnsupportedType()) {
            String typeNameBase = trinoTypeName.replaceFirst("\\(.*", "");
            String expectedMessagePart = format("(%1$s.*not (yet )?supported)|((?i)unsupported.*%1$s)|((?i)not supported.*%1$s)", Pattern.quote(typeNameBase));
            assertThatThrownBy(setup::run)
                    .hasMessageFindingMatch(expectedMessagePart)
                    .satisfies(e -> assertThat(getTrinoExceptionCause(e)).hasMessageFindingMatch(expectedMessagePart));
            return;
        }
        setup.run();

        // without pushdown, i.e. test read data mapping
        assertQuery("SELECT row_id FROM " + tableName + " WHERE rand() = 42 OR value IS NULL", "VALUES 'null value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE rand() = 42 OR value IS NOT NULL", "VALUES 'sample value', 'high value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE rand() = 42 OR value = " + sampleValueLiteral, "VALUES 'sample value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE rand() = 42 OR value = " + highValueLiteral, "VALUES 'high value'");

        assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL", "VALUES 'null value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NOT NULL", "VALUES 'sample value', 'high value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value = " + sampleValueLiteral, "VALUES 'sample value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value != " + sampleValueLiteral, "VALUES 'high value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value <= " + sampleValueLiteral, "VALUES 'sample value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value > " + sampleValueLiteral, "VALUES 'high value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value <= " + highValueLiteral, "VALUES 'sample value', 'high value'");

        assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL OR value = " + sampleValueLiteral, "VALUES 'null value', 'sample value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL OR value != " + sampleValueLiteral, "VALUES 'null value', 'high value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL OR value <= " + sampleValueLiteral, "VALUES 'null value', 'sample value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL OR value > " + sampleValueLiteral, "VALUES 'null value', 'high value'");
        assertQuery("SELECT row_id FROM " + tableName + " WHERE value IS NULL OR value <= " + highValueLiteral, "VALUES 'null value', 'sample value', 'high value'");

        assertUpdate("DROP TABLE " + tableName);
    }

    @DataProvider
    public final Object[][] testDataMappingSmokeTestDataProvider()
    {
        return testDataMappingSmokeTestData().stream()
                .map(this::filterDataMappingSmokeTestData)
                .flatMap(Optional::stream)
                .collect(toDataProvider());
    }

    private List<DataMappingTestSetup> testDataMappingSmokeTestData()
    {
        return ImmutableList.<DataMappingTestSetup>builder()
                .add(new DataMappingTestSetup("boolean", "false", "true"))
                .add(new DataMappingTestSetup("tinyint", "37", "127"))
                .add(new DataMappingTestSetup("smallint", "32123", "32767"))
                .add(new DataMappingTestSetup("integer", "1274942432", "2147483647"))
                .add(new DataMappingTestSetup("bigint", "312739231274942432", "9223372036854775807"))
                .add(new DataMappingTestSetup("real", "REAL '567.123'", "REAL '999999.999'"))
                .add(new DataMappingTestSetup("double", "DOUBLE '1234567890123.123'", "DOUBLE '9999999999999.999'"))
                .add(new DataMappingTestSetup("decimal(5,3)", "12.345", "99.999"))
                .add(new DataMappingTestSetup("decimal(15,3)", "123456789012.345", "999999999999.99"))
                .add(new DataMappingTestSetup("date", "DATE '0001-01-01'", "DATE '1582-10-04'")) // before julian->gregorian switch
                .add(new DataMappingTestSetup("date", "DATE '1582-10-05'", "DATE '1582-10-14'")) // during julian->gregorian switch
                .add(new DataMappingTestSetup("date", "DATE '2020-02-12'", "DATE '9999-12-31'"))
                .add(new DataMappingTestSetup("time", "TIME '15:03:00'", "TIME '23:59:59.999'"))
                .add(new DataMappingTestSetup("timestamp", "TIMESTAMP '2020-02-12 15:03:00'", "TIMESTAMP '2199-12-31 23:59:59.999'"))
                .add(new DataMappingTestSetup("timestamp(3) with time zone", "TIMESTAMP '2020-02-12 15:03:00 +01:00'", "TIMESTAMP '9999-12-31 23:59:59.999 +12:00'"))
                .add(new DataMappingTestSetup("char(3)", "'ab'", "'zzz'"))
                .add(new DataMappingTestSetup("varchar(3)", "'de'", "'zzz'"))
                .add(new DataMappingTestSetup("varchar", "'łąka for the win'", "'ŻŻŻŻŻŻŻŻŻŻ'"))
                .add(new DataMappingTestSetup("varbinary", "X'12ab3f'", "X'ffffffffffffffffffff'"))
                .build();
    }

    protected Optional<DataMappingTestSetup> filterDataMappingSmokeTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        return Optional.of(dataMappingTestSetup);
    }

    @Test(dataProvider = "testCaseSensitiveDataMappingProvider")
    public void testCaseSensitiveDataMapping(DataMappingTestSetup dataMappingTestSetup)
    {
        testDataMapping(dataMappingTestSetup);
    }

    @DataProvider
    public final Object[][] testCaseSensitiveDataMappingProvider()
    {
        return testCaseSensitiveDataMappingData().stream()
                .map(this::filterCaseSensitiveDataMappingTestData)
                .flatMap(Optional::stream)
                .collect(toDataProvider());
    }

    protected Optional<DataMappingTestSetup> filterCaseSensitiveDataMappingTestData(DataMappingTestSetup dataMappingTestSetup)
    {
        return Optional.of(dataMappingTestSetup);
    }

    private List<DataMappingTestSetup> testCaseSensitiveDataMappingData()
    {
        return ImmutableList.<DataMappingTestSetup>builder()
                .add(new DataMappingTestSetup("char(1)", "'A'", "'a'"))
                .add(new DataMappingTestSetup("varchar(1)", "'A'", "'a'"))
                .add(new DataMappingTestSetup("char(1)", "'A'", "'b'"))
                .add(new DataMappingTestSetup("varchar(1)", "'A'", "'b'"))
                .add(new DataMappingTestSetup("char(1)", "'B'", "'a'"))
                .add(new DataMappingTestSetup("varchar(1)", "'B'", "'a'"))
                .build();
    }

    protected static final class DataMappingTestSetup
    {
        private final String trinoTypeName;
        private final String sampleValueLiteral;
        private final String highValueLiteral;

        private final boolean unsupportedType;

        public DataMappingTestSetup(String trinoTypeName, String sampleValueLiteral, String highValueLiteral)
        {
            this(trinoTypeName, sampleValueLiteral, highValueLiteral, false);
        }

        private DataMappingTestSetup(String trinoTypeName, String sampleValueLiteral, String highValueLiteral, boolean unsupportedType)
        {
            this.trinoTypeName = requireNonNull(trinoTypeName, "trinoTypeName is null");
            this.sampleValueLiteral = requireNonNull(sampleValueLiteral, "sampleValueLiteral is null");
            this.highValueLiteral = requireNonNull(highValueLiteral, "highValueLiteral is null");
            this.unsupportedType = unsupportedType;
        }

        public String getTrinoTypeName()
        {
            return trinoTypeName;
        }

        public String getSampleValueLiteral()
        {
            return sampleValueLiteral;
        }

        public String getHighValueLiteral()
        {
            return highValueLiteral;
        }

        public boolean isUnsupportedType()
        {
            return unsupportedType;
        }

        public DataMappingTestSetup asUnsupported()
        {
            return new DataMappingTestSetup(
                    trinoTypeName,
                    sampleValueLiteral,
                    highValueLiteral,
                    true);
        }

        @Override
        public String toString()
        {
            // toString is brief because it's used for test case labels in IDE
            return trinoTypeName + (unsupportedType ? "!" : "");
        }
    }
}
