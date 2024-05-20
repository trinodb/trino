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
package io.trino.plugin.blackhole;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.trino.plugin.blackhole.BlackHoleConnector.FIELD_LENGTH_PROPERTY;
import static io.trino.plugin.blackhole.BlackHoleConnector.PAGES_PER_SPLIT_PROPERTY;
import static io.trino.plugin.blackhole.BlackHoleConnector.PAGE_PROCESSING_DELAY;
import static io.trino.plugin.blackhole.BlackHoleConnector.ROWS_PER_PAGE_PROPERTY;
import static io.trino.plugin.blackhole.BlackHoleConnector.SPLIT_COUNT_PROPERTY;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.IntStream.range;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestBlackHoleSmoke
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return BlackHoleQueryRunner.createQueryRunner();
    }

    @AfterAll
    public void tearDown()
    {
        assertThat(listBlackHoleTables()).isEmpty();
    }

    @Test
    public void testCreateSchema()
    {
        assertThat(query("SHOW SCHEMAS FROM blackhole"))
                .result().rowCount().isEqualTo(2);
        assertUpdate("CREATE TABLE test_schema as SELECT * FROM tpch.tiny.nation", 25);

        assertUpdate("CREATE SCHEMA blackhole.test");
        assertThat(query("SHOW SCHEMAS FROM blackhole"))
                .result().rowCount().isEqualTo(3);
        assertUpdate("CREATE TABLE test.test_schema as SELECT * FROM tpch.tiny.region", 5);

        assertUpdate("DROP TABLE test_schema");
        assertUpdate("DROP TABLE test.test_schema");
    }

    @Test
    public void createTableWhenTableIsAlreadyCreated()
    {
        String createTableSql = "CREATE TABLE nation as SELECT * FROM tpch.tiny.nation";
        assertUpdate(createTableSql, 25);
        assertThat(query(createTableSql))
                .failure().hasMessage("line 1:1: Destination table 'blackhole.default.nation' already exists");
        assertUpdate("DROP TABLE nation");
    }

    @Test
    public void blackHoleConnectorUsage()
    {
        assertUpdate("CREATE TABLE nation as SELECT * FROM tpch.tiny.nation", 25);

        List<QualifiedObjectName> tableNames = listBlackHoleTables();
        assertThat(tableNames).hasSize(1);
        assertThat(tableNames.get(0).objectName()).isEqualTo("nation");

        assertUpdate("INSERT INTO nation SELECT * FROM tpch.tiny.nation", 25);

        assertUpdate("INSERT INTO nation SELECT * FROM tpch.tiny.nation", 25);

        assertQuery("SELECT count(*) FROM nation", "SELECT 0");

        assertUpdate("DROP TABLE nation");
    }

    @Test
    public void notAllPropertiesSetForDataGeneration()
    {
        assertThat(query(
                format("CREATE TABLE nation WITH ( %s = 3, %s = 1 ) as SELECT * FROM tpch.tiny.nation",
                        ROWS_PER_PAGE_PROPERTY,
                        SPLIT_COUNT_PROPERTY)))
                .failure().hasMessage("All properties [split_count, pages_per_split, rows_per_page] must be set if any are set");
    }

    @Test
    public void createTableWithDistribution()
    {
        assertUpdate(
                "CREATE TABLE distributed_test WITH ( distributed_on = array['orderkey'] ) AS SELECT * FROM tpch.tiny.orders",
                15000);
        assertUpdate("DROP TABLE distributed_test");
    }

    @Test
    public void testCreateTableInNotExistSchema()
    {
        int tablesBeforeCreate = listBlackHoleTables().size();

        String createTableSql = "CREATE TABLE schema1.test_table (x date)";
        assertThat(query(createTableSql))
                .failure()
                .hasMessage("Schema schema1 not found");

        int tablesAfterCreate = listBlackHoleTables().size();
        assertThat(tablesBeforeCreate).isEqualTo(tablesAfterCreate);
    }

    @Test
    public void dataGenerationUsage()
    {
        Session session = testSessionBuilder()
                .setCatalog("blackhole")
                .setSchema("default")
                .build();

        assertUpdate(
                session,
                format("CREATE TABLE nation WITH ( %s = 3, %s = 2, %s = 1 ) as SELECT * FROM tpch.tiny.nation",
                        ROWS_PER_PAGE_PROPERTY,
                        PAGES_PER_SPLIT_PROPERTY,
                        SPLIT_COUNT_PROPERTY),
                25);
        assertQuery(session, "SELECT count(*) FROM nation", "SELECT 6");
        assertUpdate(session, "INSERT INTO nation SELECT * FROM tpch.tiny.nation", 25);
        assertQuery(session, "SELECT count(*) FROM nation", "SELECT 6");

        MaterializedResult rows = computeActual(session, "SELECT * FROM nation LIMIT 1");
        assertThat(rows.getRowCount()).isEqualTo(1);
        MaterializedRow row = Iterables.getOnlyElement(rows);
        assertThat(row.getFieldCount()).isEqualTo(4);
        assertThat(row.getField(0)).isEqualTo(0L);
        assertThat(row.getField(1)).isEqualTo("****************");
        assertThat(row.getField(2)).isEqualTo(0L);
        assertThat(row.getField(3)).isEqualTo("****************");

        assertUpdate("DROP TABLE nation");
    }

    @Test
    public void testCreateViewWithComment()
    {
        String viewName = "test_crerate_view_with_comment_" + randomNameSuffix();
        assertUpdate("CREATE VIEW " + viewName + " COMMENT 'test comment' AS SELECT * FROM tpch.tiny.nation");

        assertThat(getTableComment(viewName)).isEqualTo("test comment");

        assertUpdate("DROP VIEW " + viewName);
    }

    @Test
    public void testCommentOnView()
    {
        String viewName = "test_comment_on_view_" + randomNameSuffix();
        assertUpdate("CREATE VIEW " + viewName + " AS SELECT * FROM tpch.tiny.nation");

        // comment set
        assertUpdate("COMMENT ON VIEW " + viewName + " IS 'new comment'");
        assertThat(getTableComment(viewName)).isEqualTo("new comment");

        // comment deleted
        assertUpdate("COMMENT ON VIEW " + viewName + " IS NULL");
        assertThat(getTableComment(viewName)).isEqualTo(null);

        // comment set to non-empty value before verifying setting empty comment
        assertUpdate("COMMENT ON VIEW " + viewName + " IS 'updated comment'");
        assertThat(getTableComment(viewName)).isEqualTo("updated comment");

        // comment set to empty
        assertUpdate("COMMENT ON VIEW " + viewName + " IS ''");
        assertThat(getTableComment(viewName)).isEqualTo("");

        assertUpdate("DROP VIEW " + viewName);
    }

    private String getTableComment(String tableName)
    {
        return (String) computeScalar("SELECT comment FROM system.metadata.table_comments " +
                "WHERE catalog_name = CURRENT_CATALOG AND schema_name = CURRENT_SCHEMA AND table_name = '" + tableName + "'");
    }

    @Test
    public void fieldLength()
    {
        Session session = testSessionBuilder()
                .setCatalog("blackhole")
                .setSchema("default")
                .build();

        assertUpdate(
                session,
                format("CREATE TABLE nation WITH ( %s = 8, %s = 1, %s = 1, %s = 1 ) AS " +
                                "SELECT nationkey, name, regionkey, comment, 'abc' short_varchar FROM tpch.tiny.nation",
                        FIELD_LENGTH_PROPERTY,
                        ROWS_PER_PAGE_PROPERTY,
                        PAGES_PER_SPLIT_PROPERTY,
                        SPLIT_COUNT_PROPERTY),
                25);

        MaterializedResult rows = computeActual(session, "SELECT * FROM nation");
        assertThat(rows.getRowCount()).isEqualTo(1);
        MaterializedRow row = Iterables.getOnlyElement(rows);
        assertThat(row.getFieldCount()).isEqualTo(5);
        assertThat(row.getField(0)).isEqualTo(0L);
        assertThat(row.getField(1)).isEqualTo("********");
        assertThat(row.getField(2)).isEqualTo(0L);
        assertThat(row.getField(3)).isEqualTo("********");
        assertThat(row.getField(4)).isEqualTo("***"); // this one is shorter due to column type being VARCHAR(3)

        assertUpdate("DROP TABLE nation");
    }

    @Test
    public void testInsertAllTypes()
    {
        createBlackholeAllTypesTable();
        assertUpdate(
                "INSERT INTO blackhole_all_types VALUES (" +
                        "'abc', " +
                        "BIGINT '1', " +
                        "INTEGER '2', " +
                        "SMALLINT '3', " +
                        "TINYINT '4', " +
                        "REAL '5.1', " +
                        "DOUBLE '5.2', " +
                        "true, " +
                        "DATE '2014-01-02', " +
                        "TIMESTAMP '2014-01-02 12:12', " +
                        "cast('bar' as varbinary), " +
                        "DECIMAL '3.14', " +
                        "DECIMAL '1234567890.123456789')", 1);
        dropBlackholeAllTypesTable();
    }

    @Test
    public void testSelectAllTypes()
    {
        createBlackholeAllTypesTable();
        MaterializedResult rows = computeActual("SELECT * FROM blackhole_all_types");
        assertThat(rows.getRowCount()).isEqualTo(1);
        MaterializedRow row = Iterables.getOnlyElement(rows);
        assertThat(row.getFieldCount()).isEqualTo(13);
        assertThat(row.getField(0)).isEqualTo("**********");
        assertThat(row.getField(1)).isEqualTo(0L);
        assertThat(row.getField(2)).isEqualTo(0);
        assertThat(row.getField(3)).isEqualTo((short) 0);
        assertThat(row.getField(4)).isEqualTo((byte) 0);
        assertThat(row.getField(5)).isEqualTo(0.0f);
        assertThat(row.getField(6)).isEqualTo(0.0);
        assertThat(row.getField(7)).isEqualTo(false);
        assertThat(row.getField(8)).isEqualTo(LocalDate.ofEpochDay(0));
        assertThat(row.getField(9)).isEqualTo(LocalDateTime.of(1970, 1, 1, 0, 0, 0));
        assertThat(row.getField(10)).isEqualTo("****************".getBytes(UTF_8));
        assertThat(row.getField(11)).isEqualTo(new BigDecimal("0.00"));
        assertThat(row.getField(12)).isEqualTo(new BigDecimal("00000000000000000000.0000000000"));
        dropBlackholeAllTypesTable();
    }

    @Test
    public void testSelectWithUnenforcedConstraint()
    {
        createBlackholeAllTypesTable();
        MaterializedResult rows = computeActual("SELECT * FROM blackhole_all_types where _bigint > 10");
        assertThat(rows.getRowCount()).isEqualTo(0);
        dropBlackholeAllTypesTable();
    }

    private void createBlackholeAllTypesTable()
    {
        assertUpdate(
                format("CREATE TABLE blackhole_all_types (" +
                                "  _varchar VARCHAR(10)" +
                                ", _bigint BIGINT" +
                                ", _integer INTEGER" +
                                ", _smallint SMALLINT" +
                                ", _tinyint TINYINT" +
                                ", _real REAL" +
                                ", _double DOUBLE" +
                                ", _boolean BOOLEAN" +
                                ", _date DATE" +
                                ", _timestamp TIMESTAMP" +
                                ", _varbinary VARBINARY" +
                                ", _decimal_short DECIMAL(3,2)" +
                                ", _decimal_long DECIMAL(30,10)" +
                                ") WITH ( %s = 1, %s = 1, %s = 1 ) ",
                        ROWS_PER_PAGE_PROPERTY,
                        PAGES_PER_SPLIT_PROPERTY,
                        SPLIT_COUNT_PROPERTY));
    }

    private void dropBlackholeAllTypesTable()
    {
        assertUpdate("DROP TABLE IF EXISTS blackhole_all_types");
    }

    @Test
    public void pageProcessingDelay()
    {
        Session session = testSessionBuilder()
                .setCatalog("blackhole")
                .setSchema("default")
                .build();

        Duration pageProcessingDelay = new Duration(1, SECONDS);

        assertUpdate(
                session,
                format("CREATE TABLE nation WITH ( %s = 8, %s = 1, %s = 1, %s = 1, %s = '%s' ) AS " +
                                "SELECT * FROM tpch.tiny.nation",
                        FIELD_LENGTH_PROPERTY,
                        ROWS_PER_PAGE_PROPERTY,
                        PAGES_PER_SPLIT_PROPERTY,
                        SPLIT_COUNT_PROPERTY,
                        PAGE_PROCESSING_DELAY,
                        pageProcessingDelay),
                25);

        Stopwatch stopwatch = Stopwatch.createStarted();

        assertThat(query(session, "SELECT * FROM nation"))
                .result().rowCount().isEqualTo(1);
        assertUpdate(session, "INSERT INTO nation SELECT CAST(null AS BIGINT), CAST(null AS VARCHAR(25)), CAST(null AS BIGINT), CAST(null AS VARCHAR(152))", 1);

        stopwatch.stop();
        assertGreaterThan(stopwatch.elapsed(MILLISECONDS), pageProcessingDelay.toMillis());

        assertUpdate("DROP TABLE nation");
    }

    @Test
    public void testMultipleSplits()
    {
        assertUpdate("CREATE TABLE table_multiple_splits (a integer) WITH (split_count = 5, pages_per_split = 3, rows_per_page = 2)");

        assertThat(query("TABLE table_multiple_splits"))
                .matches("SELECT 0 FROM TABLE(sequence(1, 2 * 3 * 5))");

        assertThat(query(range(0, 7)
                .mapToObj(i -> "SELECT * FROM table_multiple_splits")
                .collect(joining(" UNION ALL "))))
                .matches("SELECT 0 FROM TABLE(sequence(1, 2 * 3 * 5 * 7))");

        assertUpdate("DROP TABLE table_multiple_splits");
    }

    private List<QualifiedObjectName> listBlackHoleTables()
    {
        QueryRunner queryRunner = getQueryRunner();
        return queryRunner.listTables(queryRunner.getDefaultSession(), "blackhole", "default");
    }
}
