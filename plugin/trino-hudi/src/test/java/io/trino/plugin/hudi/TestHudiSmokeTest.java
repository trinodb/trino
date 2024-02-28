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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;

import static io.trino.plugin.hudi.HudiQueryRunner.createHudiQueryRunner;
import static io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer.TestingTable.HUDI_COW_PT_TBL;
import static io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer.TestingTable.HUDI_NON_PART_COW;
import static io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer.TestingTable.STOCK_TICKS_COW;
import static io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer.TestingTable.STOCK_TICKS_MOR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiSmokeTest
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createHudiQueryRunner(ImmutableMap.of(), ImmutableMap.of(), new ResourceHudiTablesInitializer());
    }

    @Test
    public void testReadNonPartitionedTable()
    {
        assertQuery(
                "SELECT id, name FROM " + HUDI_NON_PART_COW,
                "SELECT * FROM VALUES (1, 'a1'), (2, 'a2')");
    }

    @Test
    public void testReadPartitionedTables()
    {
        assertQuery("SELECT symbol, max(ts) FROM " + STOCK_TICKS_COW + " GROUP BY symbol HAVING symbol = 'GOOG'",
                "SELECT * FROM VALUES ('GOOG', '2018-08-31 10:59:00')");

        assertQuery("SELECT symbol, max(ts) FROM " + STOCK_TICKS_MOR + " GROUP BY symbol HAVING symbol = 'GOOG'",
                "SELECT * FROM VALUES ('GOOG', '2018-08-31 10:59:00')");

        assertQuery("SELECT dt, count(1) FROM " + STOCK_TICKS_MOR + " GROUP BY dt",
                "SELECT * FROM VALUES ('2018-08-31', '99')");
    }

    @Test
    public void testMultiPartitionedTable()
    {
        assertQuery("SELECT _hoodie_partition_path, id, name, ts, dt, hh FROM " + HUDI_COW_PT_TBL + " WHERE id = 1",
                "SELECT * FROM VALUES ('dt=2021-12-09/hh=10', 1, 'a1', 1000, '2021-12-09', '10')");
        assertQuery("SELECT _hoodie_partition_path, id, name, ts, dt, hh FROM " + HUDI_COW_PT_TBL + " WHERE id = 2",
                "SELECT * FROM VALUES ('dt=2021-12-09/hh=11', 2, 'a2', 1000, '2021-12-09', '11')");
    }

    @Test
    public void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE " + STOCK_TICKS_COW).getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.stock_ticks_cow \\Q(\n" +
                        "   _hoodie_commit_time varchar,\n" +
                        "   _hoodie_commit_seqno varchar,\n" +
                        "   _hoodie_record_key varchar,\n" +
                        "   _hoodie_partition_path varchar,\n" +
                        "   _hoodie_file_name varchar,\n" +
                        "   volume bigint,\n" +
                        "   ts varchar,\n" +
                        "   symbol varchar,\n" +
                        "   year integer,\n" +
                        "   month varchar,\n" +
                        "   high double,\n" +
                        "   low double,\n" +
                        "   key varchar,\n" +
                        "   date varchar,\n" +
                        "   close double,\n" +
                        "   open double,\n" +
                        "   day varchar,\n" +
                        "   dt varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = \\E'.*/stock_ticks_cow',\n\\Q" +
                        "   partitioned_by = ARRAY['dt']\n" +
                        ")");
        // multi-partitioned table
        assertThat((String) computeActual("SHOW CREATE TABLE " + HUDI_COW_PT_TBL).getOnlyValue())
                .matches("CREATE TABLE \\w+\\.\\w+\\.hudi_cow_pt_tbl \\Q(\n" +
                        "   _hoodie_commit_time varchar,\n" +
                        "   _hoodie_commit_seqno varchar,\n" +
                        "   _hoodie_record_key varchar,\n" +
                        "   _hoodie_partition_path varchar,\n" +
                        "   _hoodie_file_name varchar,\n" +
                        "   id bigint,\n" +
                        "   name varchar,\n" +
                        "   ts bigint,\n" +
                        "   dt varchar,\n" +
                        "   hh varchar\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = \\E'.*/hudi_cow_pt_tbl',\n\\Q" +
                        "   partitioned_by = ARRAY['dt','hh']\n" +
                        ")");
    }

    @Test
    public void testMetaColumns()
    {
        assertQuery("SELECT _hoodie_commit_time FROM hudi_cow_pt_tbl", "VALUES ('20220906063435640'), ('20220906063456550')");
        assertQuery("SELECT _hoodie_commit_seqno FROM hudi_cow_pt_tbl", "VALUES ('20220906063435640_0_0'), ('20220906063456550_0_0')");
        assertQuery("SELECT _hoodie_record_key FROM hudi_cow_pt_tbl", "VALUES ('id:1'), ('id:2')");
        assertQuery("SELECT _hoodie_partition_path FROM hudi_cow_pt_tbl", "VALUES ('dt=2021-12-09/hh=10'), ('dt=2021-12-09/hh=11')");
        assertQuery(
                "SELECT _hoodie_file_name FROM hudi_cow_pt_tbl",
                "VALUES ('719c3273-2805-4124-b1ac-e980dada85bf-0_0-27-1215_20220906063435640.parquet'), ('4a3fcb9b-65eb-4f6e-acf9-7b0764bb4dd1-0_0-70-2444_20220906063456550.parquet')");
    }

    @Test
    public void testPathColumn()
            throws Exception
    {
        String path = (String) computeScalar("SELECT \"$path\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1");
        assertThat(toInputFile(path).exists()).isTrue();
    }

    @Test
    public void testFileSizeColumn()
            throws Exception
    {
        String path = (String) computeScalar("SELECT \"$path\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1");
        long fileSize = (long) computeScalar("SELECT \"$file_size\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1");
        assertThat(fileSize).isEqualTo(toInputFile(path).length());
    }

    @Test
    public void testFileModifiedColumn()
            throws Exception
    {
        String path = (String) computeScalar("SELECT \"$path\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1");
        ZonedDateTime fileModifiedTime = (ZonedDateTime) computeScalar("SELECT \"$file_modified_time\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1");
        assertThat(fileModifiedTime.toInstant().toEpochMilli())
                .isEqualTo(toInputFile(path).lastModified().toEpochMilli());
    }

    @Test
    public void testPartitionColumn()
    {
        assertQuery("SELECT \"$partition\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 1", "VALUES 'dt=2021-12-09/hh=10'");
        assertQuery("SELECT \"$partition\" FROM " + HUDI_COW_PT_TBL + " WHERE id = 2", "VALUES 'dt=2021-12-09/hh=11'");

        assertQueryFails("SELECT \"$partition\" FROM " + HUDI_NON_PART_COW, ".* Column '\\$partition' cannot be resolved");
    }

    @Test
    public void testPartitionFilterRequired()
    {
        Session session = withPartitionFilterRequired(getSession());

        assertQueryFails(
                session,
                "SELECT * FROM " + HUDI_COW_PT_TBL,
                "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh");
    }

    @Test
    public void testPartitionFilterRequiredPredicateOnNonPartitionColumn()
    {
        Session session = withPartitionFilterRequired(getSession());

        assertQueryFails(
                session,
                "SELECT * FROM " + HUDI_COW_PT_TBL + " WHERE id = 1",
                "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh");
    }

    @Test
    public void testPartitionFilterRequiredNestedQueryWithInnerPartitionPredicate()
    {
        Session session = withPartitionFilterRequired(getSession());

        assertQuery(session, "SELECT name FROM (SELECT * FROM " + HUDI_COW_PT_TBL + " WHERE dt = '2021-12-09') WHERE id = 1", "VALUES 'a1'");
    }

    @Test
    public void testPartitionFilterRequiredNestedQueryWithOuterPartitionPredicate()
    {
        Session session = withPartitionFilterRequired(getSession());

        assertQuery(session, "SELECT name FROM (SELECT * FROM " + HUDI_COW_PT_TBL + " WHERE id = 1) WHERE dt = '2021-12-09'", "VALUES 'a1'");
    }

    @Test
    public void testPartitionFilterRequiredNestedWithIsNotNullFilter()
    {
        Session session = withPartitionFilterRequired(getSession());

        assertQuery(session, "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE dt IS NOT null", "VALUES 'a1', 'a2'");
    }

    @Test
    public void testPartitionFilterRequiredFilterRemovedByPlanner()
    {
        Session session = withPartitionFilterRequired(getSession());

        assertQueryFails(
                session,
                "SELECT id FROM " + HUDI_COW_PT_TBL + " WHERE dt IS NOT null OR true",
                "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh");
    }

    @Test
    public void testPartitionFilterRequiredOnJoin()
    {
        Session session = withPartitionFilterRequired(getSession());
        @Language("RegExp") String errorMessage = "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh";

        // ON with partition column
        assertQueryFails(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.dt = t2.dt)",
                errorMessage);
        // ON with partition column and WHERE with same left table's partition column
        assertQuery(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.dt = t2.dt) WHERE t1.dt = '2021-12-09'",
                "VALUES ('a1', 'a1'), ('a2', 'a2'), ('a1', 'a2'), ('a2', 'a1')");
        // ON with partition column and WHERE with same right table's regular column
        assertQuery(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.dt = t2.dt) WHERE t2.dt = '2021-12-09'",
                "VALUES ('a1', 'a1'), ('a2', 'a2'), ('a1', 'a2'), ('a2', 'a1')");
        // ON with partition column and WHERE with different left table's partition column
        assertQuery(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.dt = t2.dt) WHERE t1.hh = '10'",
                "VALUES ('a1', 'a1'), ('a1', 'a2')");
        // ON with partition column and WHERE with different regular column
        assertQueryFails(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.dt = t2.dt) WHERE t2.hh = '10'",
                errorMessage);
        // ON with partition column and WHERE with regular column
        assertQueryFails(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.dt = t2.dt) WHERE t1.id = 1",
                errorMessage);

        // ON with regular column
        assertQueryFails(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.id = t2.id)",
                errorMessage);
        // ON with regular column and WHERE with left table's partition column
        assertQuery(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.id = t2.id) WHERE t1.dt = '2021-12-09'",
                "VALUES ('a1', 'a1'), ('a2', 'a2')");
        // ON with partition column and WHERE with right table's regular column
        assertQueryFails(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_NON_PART_COW + " t2 ON (t1.dt = t2.dt) WHERE t2.id = 1",
                errorMessage);
    }

    @Test
    public void testPartitionFilterRequiredOnJoinBothTablePartitioned()
    {
        Session session = withPartitionFilterRequired(getSession());

        // ON with partition column
        assertQueryFails(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.dt = t2.dt)",
                "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh");
        // ON with partition column and WHERE with same left table's partition column
        assertQuery(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.dt = t2.dt) WHERE t1.dt = '2021-12-09'",
                "VALUES ('a1', 'a1'), ('a2', 'a2'), ('a1', 'a2'), ('a2', 'a1')");
        // ON with partition column and WHERE with same right table's partition column
        assertQuery(
                session,
                "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.dt = t2.dt) WHERE t2.dt = '2021-12-09'",
                "VALUES ('a1', 'a1'), ('a2', 'a2'), ('a1', 'a2'), ('a2', 'a1')");

        @Language("RegExp") String errorMessage = "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh";
        // ON with partition column and WHERE with different left table's partition column
        assertQueryFails(session, "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.dt = t2.dt) WHERE t1.hh = '10'", errorMessage);
        // ON with partition column and WHERE with different right table's partition column
        assertQueryFails(session, "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.dt = t2.dt) WHERE t2.hh = '10'", errorMessage);
        // ON with partition column and WHERE with regular column
        assertQueryFails(session, "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.dt = t2.dt) WHERE t2.id = 1", errorMessage);

        // ON with regular column
        assertQueryFails(session, "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.id = t2.id)", errorMessage);
        // ON with regular column and WHERE with regular column
        assertQueryFails(session, "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.id = t2.id) WHERE t1.id = 1", errorMessage);
        // ON with regular column and WHERE with left table's partition column
        assertQueryFails(session, "SELECT t1.name, t2.name FROM " + HUDI_COW_PT_TBL + " t1 JOIN " + HUDI_COW_PT_TBL + " t2 ON (t1.id = t2.id) WHERE t1.dt = '2021-12-09'", errorMessage);
    }

    @Test
    public void testPartitionFilterRequiredWithLike()
    {
        Session session = withPartitionFilterRequired(getSession());
        assertQueryFails(
                session,
                "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE name LIKE '%1'",
                "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh");
    }

    @Test
    public void testPartitionFilterRequiredFilterIncluded()
    {
        Session session = withPartitionFilterRequired(getSession());
        assertQuery(session, "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE hh = '10'", "VALUES 'a1'");
        assertQuery(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE hh < '12'", "VALUES 2");
        assertQuery(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE Hh < '11'", "VALUES 1");
        assertQuery(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE HH < '10'", "VALUES 0");
        assertQuery(session, "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE CAST(hh AS INTEGER) % 2 = 1 and hh IS NOT NULL", "VALUES 'a2'");
        assertQuery(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE hh IS NULL", "VALUES 0");
        assertQuery(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE hh IS NOT NULL", "VALUES 2");
        assertQuery(session, "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE hh LIKE '10'", "VALUES 'a1'");
        assertQuery(session, "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE hh LIKE '1%'", "VALUES 'a1', 'a2'");
        assertQuery(session, "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE id = 1 AND dt = '2021-12-09'", "VALUES 'a1'");
        assertQuery(session, "SELECT name FROM " + HUDI_COW_PT_TBL + " WHERE hh = '11' AND dt = '2021-12-09'", "VALUES 'a2'");
        assertQuery(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE hh = '12' AND dt = '2021-12-19'", "VALUES 0");

        // Predicate which could not be translated into tuple domain
        @Language("RegExp") String errorMessage = "Filter required on tests." + HUDI_COW_PT_TBL.getTableName() + " for at least one of the partition columns: dt, hh";
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE CAST(hh AS INTEGER) % 2 = 0", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE CAST(hh AS INTEGER) - 11 = 0", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE CAST(hh AS INTEGER) * 2 = 20", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE CAST(hh AS INTEGER) % 2 > 0", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE name LIKE '%1' OR hh LIKE '%1'", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE name LIKE '%1' AND hh LIKE '%0'", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE id = 1 OR dt = '2021-12-09'", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE hh = '11' OR dt = '2021-12-09'", errorMessage);
        assertQueryFails(session, "SELECT count(*) FROM " + HUDI_COW_PT_TBL + " WHERE hh = '12' OR dt = '2021-12-19'", errorMessage);
        assertQueryFails(session, "SELECT count(*) AS COUNT FROM " + HUDI_COW_PT_TBL + " WHERE CAST(hh AS INTEGER) > 2 GROUP BY name ", errorMessage);
    }

    private static Session withPartitionFilterRequired(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "query_partition_filter_required", "true")
                .build();
    }

    private TrinoInputFile toInputFile(String path)
    {
        return ((HudiConnector) getDistributedQueryRunner().getCoordinator().getConnector("hudi")).getInjector()
                .getInstance(TrinoFileSystemFactory.class)
                .create(ConnectorIdentity.ofUser("test"))
                .newInputFile(Location.of(path));
    }
}
