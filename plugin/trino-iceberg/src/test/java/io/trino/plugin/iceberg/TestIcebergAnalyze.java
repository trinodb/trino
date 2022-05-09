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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.trino.Session;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TestingTypeManager;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.StatisticsFile;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.EXECUTE_TABLE_PROCEDURE;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.tpch.TpchTable.NATION;
import static java.lang.String.format;
import static org.testng.Assert.assertNotEquals;

public class TestIcebergAnalyze
        extends AbstractTestQueryFramework
{
//    @Override
//    protected QueryRunner createQueryRunner()
//            throws Exception
//    {
//        return createIcebergQueryRunner();
//    }

    private HiveMetastore metastore;
    private HdfsEnvironment hdfsEnvironment;
    private java.nio.file.Path tempDir;
    private File metastoreDir;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        hdfsEnvironment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());

        tempDir = Files.createTempDirectory("test_iceberg_v2");
        metastoreDir = tempDir.resolve("iceberg_data").toFile();
        metastore = createTestingFileHiveMetastore(metastoreDir);

        return IcebergQueryRunner.builder()
                .setInitialTables(NATION)
                .setMetastoreDirectory(metastoreDir)
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Test
    public void testAnalyze()
    {
        String tableName = "test_analyze";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, null, 0, null, '0', '24')," +
                        "('regionkey', null, null, 0, null, '0', '4')," +
                        "('comment', null, null, 0, null, null, null)," +
                        "('name', null, null, 0, null, null, null)," +
                        "(null, null, null, null, 25, null, null)");

        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25, 0, null, '0', '24')," +
                        "('regionkey', null, 5, 0, null, '0', '4')," +
                        "('comment', null, 25, 0, null, null, null)," +
                        "('name', null, 25, 0, null, null, null)," +
                        "(null, null, null, null, 25, null, null)");

        // reanalyze data
        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25, 0, null, '0', '24')," +
                        "('regionkey', null, 5, 0, null, '0', '4')," +
                        "('comment', null, 25, 0, null, null, null)," +
                        "('name', null, 25, 0, null, null, null)," +
                        "(null, null, null, null, 25, null, null)");

        // insert one more copy; should not influence stats other than rowcount
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation", 25);

        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25, 0, null, '0', '24')," +
                        "('regionkey', null, 5, 0, null, '0', '4')," +
                        "('comment', null, 25, 0, null, null, null)," +
                        "('name', null, 25, 0, null, null, null)," +
                        "(null, null, null, null, 50, null, null)");

        // insert modified rows
        assertUpdate("INSERT INTO " + tableName + " SELECT nationkey + 25, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);

        // without ANALYZE all stats but NDV should be updated
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25, 0, null, '0', '49')," +
                        "('regionkey', null, 5, 0, null, '0', '9')," +
                        "('comment', null, 25, 0, null, null, null)," +
                        "('name', null, 25, 0, null, null, null)," +
                        "(null, null, null, null, 75, null, null)");

        // with analyze we should get new NDV
        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50, 0, null, '0', '49')," +
                        "('regionkey', null, 10, 0, null, '0', '9')," +
                        "('comment', null, 50, 0, null, null, null)," +
                        "('name', null, 50, 0, null, null, null)," +
                        "(null, null, null, null, 75, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeWithSchemaEvolution()
    {
        String tableName = "test_analyze_with_schema_evolution";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertUpdate("ANALYZE " + tableName);

        assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN info varchar");
        assertUpdate("UPDATE " + tableName + " SET info = format('%s %s', name, comment)", 25);
        assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN comment");

        // schema changed, ANALYZE hasn't been re-run yet
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25, 0, null, '0', '24')," +
                        "('regionkey', null, 5, 0, null, '0', '4')," +
                        "('name', null, 25, 0, null, null, null)," +
                        "('info', null, null, null, null, null, null)," +
                        "(null, null, null, null, 50, null, null)");

        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25, 0, null, '0', '24')," +
                        "('regionkey', null, 5, 0, null, '0', '4')," +
                        "('name', null, 25, 0, null, null, null)," +
                        "('info', null, 25, null, null, null, null)," +
                        "(null, null, null, null, 50, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzePartitioned()
    {
        String tableName = "test_analyze_partitioned";
        assertUpdate("CREATE TABLE " + tableName + " WITH (partitioning = ARRAY['regionkey']) AS SELECT * FROM tpch.sf1.nation", 25);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, null, 0, null, '0', '24')," +
                        "('regionkey', null, null, 0, null, '0', '4')," +
                        "('comment', null, null, 0, null, null, null)," +
                        "('name', null, null, 0, null, null, null)," +
                        "(null, null, null, null, 25, null, null)");

        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25, 0, null, '0', '24')," +
                        "('regionkey', null, 5, 0, null, '0', '4')," +
                        "('comment', null, 25, 0, null, null, null)," +
                        "('name', null, 25, 0, null, null, null)," +
                        "(null, null, null, null, 25, null, null)");

        // insert one more copy; should not influence stats other than rowcount
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation", 25);

        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25, 0, null, '0', '24')," +
                        "('regionkey', null, 5, 0, null, '0', '4')," +
                        "('comment', null, 25, 0, null, null, null)," +
                        "('name', null, 25, 0, null, null, null)," +
                        "(null, null, null, null, 50, null, null)");

        // insert modified rows
        assertUpdate("INSERT INTO " + tableName + " SELECT nationkey + 25, reverse(name), regionkey + 5, reverse(comment) FROM tpch.sf1.nation", 25);

        // without ANALYZE all stats but NDV should be updated
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25, 0, null, '0', '49')," +
                        "('regionkey', null, 5, 0, null, '0', '9')," +
                        "('comment', null, 25, 0, null, null, null)," +
                        "('name', null, 25, 0, null, null, null)," +
                        "(null, null, null, null, 75, null, null)");

        // with analyze we should get new NDV
        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50, 0, null, '0', '49')," +
                        "('regionkey', null, 10, 0, null, '0', '9')," +
                        "('comment', null, 50, 0, null, null, null)," +
                        "('name', null, 50, 0, null, null, null)," +
                        "(null, null, null, null, 75, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeEmpty()
    {
        String tableName = "test_analyze_empty";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation WITH NO DATA", 0);

        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', 0, 0, 1, null, null, null)," +
                        "('regionkey', 0, 0, 1, null, null, null)," +
                        "('comment', 0, 0, 1, null, null, null)," +
                        "('name', 0, 0, 1, null, null, null)," +
                        "(null, null, null, null, 0, null, null)");

        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', 0, 0, 1, null, null, null)," +
                        "('regionkey', 0, 0, 1, null, null, null)," +
                        "('comment', 0, 0, 1, null, null, null)," +
                        "('name', 0, 0, 1, null, null, null)," +
                        "(null, null, null, null, 0, null, null)");

        // add some data and reanalyze
        assertUpdate("INSERT INTO " + tableName + " SELECT * FROM tpch.sf1.nation", 25);

        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25, 0, null, '0', '24')," +
                        "('regionkey', null, 5, 0, null, '0', '4')," +
                        "('comment', null, 25, 0, null, null, null)," +
                        "('name', null, 25, 0, null, null, null)," +
                        "(null, null, null, null, 25, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeSomeColumns()
    {
        String tableName = "test_analyze_some_columns";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        // analyze NULL list of columns
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = NULL)", "\\QInvalid null value for catalog 'iceberg' analyze property 'columns' from [null]");

        // analyze empty list of columns
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY[])", "\\QCannot specify empty list of columns for analysis");

        // specify inexistent column
        assertQueryFails("ANALYZE " + tableName + " WITH (columns = ARRAY['nationkey', 'blah'])", "\\QInvalid columns specified for analysis: [blah]");

        // specify NULL column
        assertQueryFails(
                "ANALYZE " + tableName + " WITH (columns = ARRAY['nationkey', NULL])",
                "\\QUnable to set catalog 'iceberg' analyze property 'columns' to [ARRAY['nationkey',null]]: Invalid null value in analyze columns property");

        // analyze nationkey and regionkey
        assertUpdate("ANALYZE " + tableName + " WITH (columns = ARRAY['nationkey', 'regionkey'])");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 25, 0, null, '0', '24')," +
                        "('regionkey', null, 5, 0, null, '0', '4')," +
                        "('comment', null, null, 0, null, null, null)," +
                        "('name', null, null, 0, null, null, null)," +
                        "(null, null, null, null, 25, null, null)");

        // insert modified rows
        assertUpdate("INSERT INTO " + tableName + " SELECT nationkey + 25, concat(name, '1'), regionkey + 5, concat(comment, '21') FROM tpch.sf1.nation", 25);

        // perform one more analyze for nationkey and regionkey
        assertUpdate("ANALYZE " + tableName + " WITH (columns = ARRAY['nationkey', 'regionkey'])");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50, 0, null, '0', '49')," +
                        "('regionkey', null, 10, 0, null, '0', '9')," +
                        "('comment', null, null, 0, null, null, null)," +
                        "('name', null, null, 0, null, null, null)," +
                        "(null, null, null, null, 50, null, null)");

        // drop stats
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE DROP_EXTENDED_STATS");

        // analyze all columns
        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50, 0, null, '0', '49')," +
                        "('regionkey', null, 10, 0, null, '0', '9')," +
                        "('comment', null, 50, 0, null, null, null)," +
                        "('name', null, 50, 0, null, null, null)," +
                        "(null, null, null, null, 50, null, null)");

        // insert modified rows
        assertUpdate("INSERT INTO " + tableName + " SELECT nationkey + 50, concat(name, '2'), regionkey + 10, concat(comment, '22') FROM tpch.sf1.nation", 25);

        // without ANALYZE all stats but NDV should be updated
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 50, 0, null, '0', '74')," +
                        "('regionkey', null, 10, 0, null, '0', '14')," +
                        "('comment', null, 50, 0, null, null, null)," +
                        "('name', null, 50, 0, null, null, null)," +
                        "(null, null, null, null, 75, null, null)");

        // reanalyze with a subset of columns
        assertUpdate("ANALYZE " + tableName + " WITH (columns = ARRAY['nationkey', 'regionkey'])");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 75, 0, null, '0', '74')," +
                        "('regionkey', null, 15, 0, null, '0', '14')," +
                        "('comment', null, 50, 0, null, null, null)," + // result of previous analyze
                        "('name', null, 50, 0, null, null, null)," + // result of previous analyze
                        "(null, null, null, null, 75, null, null)");

        // analyze all columns
        assertUpdate("ANALYZE " + tableName);
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES " +
                        "('nationkey', null, 75, 0, null, '0', '74')," +
                        "('regionkey', null, 15, 0, null, '0', '14')," +
                        "('comment', null, 75, 0, null, null, null)," +
                        "('name', null, 75, 0, null, null, null)," +
                        "(null, null, null, null, 75, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropExtendedStats()
    {
        String tableName = "test_drop_extended_stats";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        String baseStats = "VALUES"
                + "('nationkey', null, null, 0, null, '0', '24'),"
                + "('regionkey', null, null, 0, null, '0', '4'),"
                + "('comment', null, null, 0, null, null, null),"
                + "('name',  null, null, 0, null, null, null),"
                + "(null,  null, null, null, 25, null, null)";
        String extendedStats = "VALUES"
                + "('nationkey', null, 25, 0, null, '0', '24'),"
                + "('regionkey', null, 5, 0, null, '0', '4'),"
                + "('comment', null, 25, 0, null, null, null),"
                + "('name',  null, 25, 0, null, null, null),"
                + "(null,  null, null, null, 25, null, null)";

        assertQuery("SHOW STATS FOR " + tableName, baseStats);

        // Update stats to include distinct count
        assertUpdate("ANALYZE " + tableName);
        assertQuery("SHOW STATS FOR " + tableName, extendedStats);

        // Dropping extended stats clears distinct count and leaves other stats alone
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE DROP_EXTENDED_STATS");
        assertQuery("SHOW STATS FOR " + tableName, baseStats);

        // Re-analyzing should work
        assertUpdate("ANALYZE " + tableName);
        assertQuery("SHOW STATS FOR " + tableName, extendedStats);

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropMissingStats()
    {
        String tableName = "test_drop_missing_stats";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        // When there are no extended stats, the procedure should have no effect
        assertUpdate("ALTER TABLE " + tableName + " EXECUTE DROP_EXTENDED_STATS");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES"
                        + "('nationkey', null, null, 0, null, '0', '24'),"
                        + "('regionkey', null, null, 0, null, '0', '4'),"
                        + "('comment', null, null, 0, null, null, null),"
                        + "('name',  null, null, 0, null, null, null),"
                        + "(null,  null, null, null, 25, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testDropStatsAccessControl()
    {
        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        String tableName = "test_deny_drop_stats";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);

        assertAccessDenied(
                "ALTER TABLE " + tableName + " EXECUTE DROP_EXTENDED_STATS",
                "Cannot execute table procedure DROP_EXTENDED_STATS on iceberg.tpch.test_deny_drop_stats",
                privilege(format("%s.%s.%s.DROP_EXTENDED_STATS", catalog, schema, tableName), EXECUTE_TABLE_PROCEDURE));

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testReanalyze()
    {
        String tableName = "test_reanalyze";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);
        assertUpdate("ANALYZE " + tableName);
        StatisticsFile initialStatsFile = Iterables.getOnlyElement(loadTable(tableName).currentSnapshot().statisticsFiles());
        assertUpdate("ANALYZE " + tableName);
        StatisticsFile newStatsFile = Iterables.getOnlyElement(loadTable(tableName).currentSnapshot().statisticsFiles());
        assertNotEquals(initialStatsFile.location(), newStatsFile.location());

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeAndRollbackToSnapshot()
    {
        String schema = getSession().getSchema().orElseThrow();
        String tableName = "test_analyze_and_rollback";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);
        long createSnapshot = getLatestSnapshotId(tableName);
        assertUpdate("ANALYZE " + tableName);
        long analyzeSnapshot = getLatestSnapshotId(tableName);
        assertNotEquals(analyzeSnapshot, createSnapshot);

        assertUpdate(format("CALL system.rollback_to_snapshot('%s', '%s', %s)", schema, tableName, createSnapshot));
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES"
                        // no NDV information after rollback_to_snapshot
                        + "('nationkey', null, null, 0, null, '0', '24'),"
                        + "('regionkey', null, null, 0, null, '0', '4'),"
                        + "('comment', null, null, 0, null, null, null),"
                        + "('name',  null, null, 0, null, null, null),"
                        + "(null,  null, null, null, 25, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    @Test
    public void testAnalyzeAndDeleteOrphanFiles()
    {
        String tableName = "test_analyze_and_delete_orphan_files";
        assertUpdate("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.sf1.nation", 25);
        assertUpdate("ANALYZE " + tableName);

        assertQuerySucceeds(
                Session.builder(getSession())
                        .setCatalogSessionProperty("iceberg", "delete_orphan_files_min_retention", "0s")
                        .build(),
                "ALTER TABLE " + tableName + " EXECUTE DELETE_ORPHAN_FILES (retention_threshold => '0s')");
        assertQuery(
                "SHOW STATS FOR " + tableName,
                "VALUES"
                        // NDV information still present
                        + "('nationkey', null, 25, 0, null, '0', '24'),"
                        + "('regionkey', null, 5, 0, null, '0', '4'),"
                        + "('comment', null, 25, 0, null, null, null),"
                        + "('name',  null, 25, 0, null, null, null),"
                        + "(null,  null, null, null, 25, null, null)");

        assertUpdate("DROP TABLE " + tableName);
    }

    private long getLatestSnapshotId(String tableName)
    {
        return (long) computeActual(format("SELECT snapshot_id FROM \"%s$snapshots\" ORDER BY committed_at DESC LIMIT 1", tableName))
                .getOnlyValue();
    }

    private BaseTable loadTable(String tableName)
    {
        String catalogName = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        IcebergTableOperationsProvider tableOperationsProvider = new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment));
        TrinoCatalog catalog = new TrinoHiveCatalog(
                new CatalogName("hive"),
                CachingHiveMetastore.memoizeMetastore(metastore, 1000),
                hdfsEnvironment,
                new TestingTypeManager(),
                tableOperationsProvider,
                "test",
                false,
                false,
                false);
        ConnectorSession connectorSession = getSession().toConnectorSession(catalogName);
        return (BaseTable) loadIcebergTable(catalog, tableOperationsProvider, connectorSession, new SchemaTableName(schema, tableName));
    }
}
