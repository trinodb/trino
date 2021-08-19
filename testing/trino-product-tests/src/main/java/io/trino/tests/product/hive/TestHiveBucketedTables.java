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
package io.trino.tests.product.hive;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.tempto.Requirement;
import io.trino.tempto.Requirements;
import io.trino.tempto.RequirementsProvider;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.fulfillment.table.MutableTableRequirement;
import io.trino.tempto.fulfillment.table.hive.HiveTableDefinition;
import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.anyOf;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static io.trino.tempto.fulfillment.table.MutableTableRequirement.State.PREPARED;
import static io.trino.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static io.trino.tempto.fulfillment.table.TableRequirements.immutableTable;
import static io.trino.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static io.trino.tempto.query.QueryExecutor.param;
import static io.trino.tempto.query.QueryExecutor.query;
import static io.trino.tests.product.TestGroups.BIG_QUERY;
import static io.trino.tests.product.TpchTableResults.PRESTO_NATION_RESULT;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static io.trino.tests.product.utils.TableDefinitionUtils.mutableTableInstanceOf;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.JDBCType.VARCHAR;

public class TestHiveBucketedTables
        extends HiveProductTest
        implements RequirementsProvider
{
    private static final Logger log = Logger.get(TestHiveBucketedTables.class);

    public static final HiveTableDefinition BUCKETED_NATION = bucketTableDefinition("bucket_nation", false, false);

    public static final HiveTableDefinition BUCKETED_NATION_PREPARED = HiveTableDefinition.builder("bucket_nation_prepared")
            .setCreateTableDDLTemplate("Table %NAME% should be only used with CTAS queries")
            .setNoData()
            .build();

    public static final HiveTableDefinition BUCKETED_SORTED_NATION = bucketTableDefinition("bucketed_sorted_nation", true, false);

    public static final HiveTableDefinition BUCKETED_PARTITIONED_NATION = bucketTableDefinition("bucketed_partitioned_nation", false, true);

    private static HiveTableDefinition bucketTableDefinition(String tableName, boolean sorted, boolean partitioned)
    {
        return HiveTableDefinition.builder(tableName)
                .setCreateTableDDLTemplate("CREATE TABLE %NAME%(" +
                        "n_nationkey     BIGINT," +
                        "n_name          STRING," +
                        "n_regionkey     BIGINT," +
                        "n_comment       STRING) " +
                        (partitioned ? "PARTITIONED BY (part_key STRING) " : " ") +
                        "CLUSTERED BY (n_regionkey) " +
                        (sorted ? "SORTED BY (n_regionkey) " : " ") +
                        "INTO 2 BUCKETS " +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
                        "TBLPROPERTIES ('bucketing_version'='1')")
                .setNoData()
                .build();
    }

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return Requirements.compose(
                MutableTableRequirement.builder(BUCKETED_PARTITIONED_NATION).withState(CREATED).build(),
                MutableTableRequirement.builder(BUCKETED_NATION).withState(CREATED).build(),
                MutableTableRequirement.builder(BUCKETED_NATION_PREPARED).withState(PREPARED).build(),
                MutableTableRequirement.builder(BUCKETED_SORTED_NATION).withState(CREATED).build(),
                immutableTable(NATION));
    }

    @Test
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testSelectStar()
    {
        String tableName = mutableTableInstanceOf(BUCKETED_NATION).getNameInDatabase();
        populateHiveTable(tableName, NATION.getName());

        assertThat(query("SELECT * FROM " + tableName)).matches(PRESTO_NATION_RESULT);
    }

    @Test(groups = BIG_QUERY)
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testIgnorePartitionBucketingIfNotBucketed()
    {
        String tableName = mutableTablesState().get(BUCKETED_PARTITIONED_NATION).getNameInDatabase();
        populateHivePartitionedTable(tableName, NATION.getName(), "part_key = 'insert_1'");
        populateHivePartitionedTable(tableName, NATION.getName(), "part_key = 'insert_2'");

        onHive().executeQuery(format("ALTER TABLE %s NOT CLUSTERED", tableName));

        assertThat(query(format("SELECT count(DISTINCT n_nationkey), count(*) FROM %s", tableName)))
                .hasRowsCount(1)
                .contains(row(25, 50));

        assertThat(query(format("SELECT count(*) FROM %s WHERE n_nationkey = 1", tableName)))
                .containsExactly(row(2));
    }

    @Test(groups = BIG_QUERY)
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testAllowMultipleFilesPerBucket()
    {
        String tableName = mutableTablesState().get(BUCKETED_PARTITIONED_NATION).getNameInDatabase();
        for (int i = 0; i < 3; i++) {
            populateHivePartitionedTable(tableName, NATION.getName(), "part_key = 'insert'");
        }

        assertThat(query(format("SELECT count(DISTINCT n_nationkey), count(*) FROM %s", tableName)))
                .hasRowsCount(1)
                .contains(row(25, 75));

        assertThat(query(format("SELECT count(*) FROM %s WHERE n_nationkey = 1", tableName)))
                .containsExactly(row(3));
    }

    @Test
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testSelectAfterMultipleInserts()
    {
        String tableName = mutableTableInstanceOf(BUCKETED_NATION).getNameInDatabase();
        populateHiveTable(tableName, NATION.getName());
        populateHiveTable(tableName, NATION.getName());

        assertThat(query(format("SELECT count(*) FROM %s WHERE n_nationkey = 1", tableName)))
                .containsExactly(row(2));
        assertThat(query(format("SELECT count(*) FROM %s WHERE n_regionkey = 1", tableName)))
                .containsExactly(row(10));
        assertThat(query(format("SELECT n_regionkey, count(*) FROM %s GROUP BY n_regionkey", tableName)))
                .containsOnly(row(0, 10), row(1, 10), row(2, 10), row(3, 10), row(4, 10));
        assertThat(query(format("SELECT count(*) FROM %s n JOIN %s n1 ON n.n_regionkey = n1.n_regionkey", tableName, tableName)))
                .containsExactly(row(500));
    }

    @Test
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testSelectAfterMultipleInsertsForSortedTable()
    {
        String tableName = mutableTableInstanceOf(BUCKETED_SORTED_NATION).getNameInDatabase();
        populateHiveTable(tableName, NATION.getName());
        populateHiveTable(tableName, NATION.getName());

        assertThat(query(format("SELECT count(*) FROM %s WHERE n_nationkey = 1", tableName)))
                .containsExactly(row(2));
        assertThat(query(format("SELECT count(*) FROM %s WHERE n_regionkey = 1", tableName)))
                .containsExactly(row(10));
        assertThat(query(format("SELECT n_regionkey, count(*) FROM %s GROUP BY n_regionkey", tableName)))
                .containsOnly(row(0, 10), row(1, 10), row(2, 10), row(3, 10), row(4, 10));
        assertThat(query(format("SELECT count(*) FROM %s n JOIN %s n1 ON n.n_regionkey = n1.n_regionkey", tableName, tableName)))
                .containsExactly(row(500));
    }

    @Test
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testSelectAfterMultipleInsertsForPartitionedTable()
    {
        String tableName = mutableTableInstanceOf(BUCKETED_PARTITIONED_NATION).getNameInDatabase();
        populateHivePartitionedTable(tableName, NATION.getName(), "part_key = 'insert_1'");
        populateHivePartitionedTable(tableName, NATION.getName(), "part_key = 'insert_2'");
        populateHivePartitionedTable(tableName, NATION.getName(), "part_key = 'insert_1'");
        populateHivePartitionedTable(tableName, NATION.getName(), "part_key = 'insert_2'");

        assertThat(query(format("SELECT count(*) FROM %s WHERE n_nationkey = 1", tableName)))
                .containsExactly(row(4));
        assertThat(query(format("SELECT count(*) FROM %s WHERE n_regionkey = 1", tableName)))
                .containsExactly(row(20));
        assertThat(query(format("SELECT count(*) FROM %s WHERE n_regionkey = 1 AND part_key = 'insert_1'", tableName)))
                .hasRowsCount(1)
                .containsExactly(row(10));
        assertThat(query(format("SELECT n_regionkey, count(*) FROM %s WHERE part_key = 'insert_2' GROUP BY n_regionkey", tableName)))
                .containsOnly(row(0, 10), row(1, 10), row(2, 10), row(3, 10), row(4, 10));
        assertThat(query(format("SELECT count(*) FROM %s n JOIN %s n1 ON n.n_regionkey = n1.n_regionkey", tableName, tableName)))
                .containsExactly(row(2000));
        assertThat(query(format("SELECT count(*) FROM %s n JOIN %s n1 ON n.n_regionkey = n1.n_regionkey WHERE n.part_key = 'insert_1'", tableName, tableName)))
                .containsExactly(row(1000));
    }

    @Test
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testSelectFromEmptyBucketedTableEmptyTablesAllowed()
    {
        String tableName = mutableTableInstanceOf(BUCKETED_NATION).getNameInDatabase();
        assertThat(query(format("SELECT count(*) FROM %s", tableName)))
                .containsExactly(row(0));
    }

    @Test
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testSelectFromIncompleteBucketedTableEmptyTablesAllowed()
    {
        String tableName = mutableTableInstanceOf(BUCKETED_NATION).getNameInDatabase();
        populateRowToHiveTable(tableName, ImmutableList.of("2", "'name'", "2", "'comment'"), Optional.empty());
        // insert one row into nation
        assertThat(query(format("SELECT count(*) from %s", tableName)))
                .containsExactly(row(1));
        assertThat(query(format("select n_nationkey from %s where n_regionkey = 2", tableName)))
                .containsExactly(row(2));
    }

    @Test
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testInsertPartitionedBucketed()
    {
        String tableName = mutableTablesState().get(BUCKETED_NATION_PREPARED).getNameInDatabase();

        String ctasQuery = "CREATE TABLE %s WITH (bucket_count = 4, bucketed_by = ARRAY['n_regionkey'], partitioned_by = ARRAY['part_key']) " +
                "AS SELECT n_nationkey, n_name, n_regionkey, n_comment, n_name as part_key FROM %s";
        query(format(ctasQuery, tableName, NATION.getName()));

        assertThat(query(format("SELECT count(*) FROM %s", tableName))).containsExactly(row(25));
        assertThat(query(format("SELECT count(*) FROM %s WHERE n_regionkey=0", tableName))).containsExactly(row(5));
        assertThat(query(format("SELECT count(*) FROM %s WHERE part_key='ALGERIA'", tableName))).containsExactly(row(1));
        assertThat(query(format("SELECT count(*) FROM %s WHERE n_regionkey=0 AND part_key='ALGERIA'", tableName))).containsExactly(row(1));
    }

    @Test
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testCreatePartitionedBucketedTableAsSelect()
    {
        String tableName = mutableTablesState().get(BUCKETED_PARTITIONED_NATION).getNameInDatabase();

        query(format("INSERT INTO %s SELECT n_nationkey, n_name, n_regionkey, n_comment, n_name FROM %s", tableName, NATION.getName()));

        assertThat(query(format("SELECT count(*) FROM %s", tableName))).containsExactly(row(25));
        assertThat(query(format("SELECT count(*) FROM %s WHERE n_regionkey=0", tableName))).containsExactly(row(5));
        assertThat(query(format("SELECT count(*) FROM %s WHERE part_key='ALGERIA'", tableName))).containsExactly(row(1));
        assertThat(query(format("SELECT count(*) FROM %s WHERE n_regionkey=0 AND part_key='ALGERIA'", tableName))).containsExactly(row(1));
    }

    @Test
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testInsertIntoBucketedTables()
    {
        String tableName = mutableTablesState().get(BUCKETED_NATION).getNameInDatabase();

        query(format("INSERT INTO %s SELECT * FROM %s", tableName, NATION.getName()));
        // make sure that insert will not overwrite existing data
        query(format("INSERT INTO %s SELECT * FROM %s", tableName, NATION.getName()));

        assertThat(query(format("SELECT count(*) FROM %s", tableName))).containsExactly(row(50));
        assertThat(query(format("SELECT count(*) FROM %s WHERE n_regionkey=0", tableName))).containsExactly(row(10));
    }

    @Test
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testCreateBucketedTableAsSelect()
    {
        String tableName = mutableTablesState().get(BUCKETED_NATION_PREPARED).getNameInDatabase();

        // nations has 25 rows and NDV=5 for n_regionkey, setting bucket_count=10 will surely create empty buckets
        query(format("CREATE TABLE %s WITH (bucket_count = 10, bucketed_by = ARRAY['n_regionkey']) AS SELECT * FROM %s", tableName, NATION.getName()));

        assertThat(query(format("SELECT * FROM %s", tableName))).matches(PRESTO_NATION_RESULT);
        assertThat(query(format("SELECT count(*) FROM %s WHERE n_regionkey=0", tableName))).containsExactly(row(5));
    }

    @Test
    @Flaky(issue = ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE, match = ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
    public void testBucketingVersion()
    {
        String value = "Trino rocks";
        String bucketV1 = "000002_0";
        String bucketV2Standard = "000001_0";
        String bucketV2DirectInsert = "bucket_00001";

        List<String> bucketV1NameOptions = ImmutableList.of(bucketV1);
        List<String> bucketV2NameOptions = ImmutableList.of(bucketV2Standard, bucketV2DirectInsert);

        testBucketingVersion(BucketingType.BUCKETED_DEFAULT, value, false, (getHiveVersionMajor() < 3) ? bucketV1NameOptions : bucketV2NameOptions);
        testBucketingVersion(BucketingType.BUCKETED_DEFAULT, value, true, (getHiveVersionMajor() < 3) ? bucketV1NameOptions : bucketV2NameOptions);
        testBucketingVersion(BucketingType.BUCKETED_V1, value, false, bucketV1NameOptions);
        testBucketingVersion(BucketingType.BUCKETED_V1, value, true, bucketV1NameOptions);
        if (getHiveVersionMajor() >= 3) {
            testBucketingVersion(BucketingType.BUCKETED_V2, value, false, bucketV2NameOptions);
            testBucketingVersion(BucketingType.BUCKETED_V2, value, true, bucketV2NameOptions);
        }
    }

    private void testBucketingVersion(BucketingType bucketingType, String value, boolean insertWithTrino, List<String> expectedFileNameOptions)
    {
        log.info("Testing with bucketingType=%s, value='%s', insertWithTrino=%s, expectedFileNamePossibilites=%s", bucketingType, value, insertWithTrino, expectedFileNameOptions);

        onHive().executeQuery("DROP TABLE IF EXISTS test_bucketing_version");
        onHive().executeQuery("" +
                "CREATE TABLE test_bucketing_version(a string) " +
                bucketingType.getHiveClustering("a", 4) + " " +
                "STORED AS ORC " +
                hiveTableProperties(bucketingType));

        if (insertWithTrino) {
            onTrino().executeQuery("INSERT INTO test_bucketing_version(a) VALUES (?)", param(VARCHAR, value));
        }
        else {
            onHive().executeQuery("SET hive.enforce.bucketing = true");
            onHive().executeQuery("INSERT INTO test_bucketing_version(a) VALUES ('" + value + "')");
        }

        assertThat(onTrino().executeQuery("SELECT a, regexp_extract(\"$path\", '^.*/([^_/]+_[^_/]+)(_[^/]+)?$', 1) FROM test_bucketing_version"))
                .containsOnly(row(value, anyOf(expectedFileNameOptions.toArray())));
    }

    private String hiveTableProperties(BucketingType bucketingType)
    {
        ImmutableList.Builder<String> tableProperties = ImmutableList.builder();
        tableProperties.add("'transactional'='false'");
        tableProperties.addAll(bucketingType.getHiveTableProperties());
        return "TBLPROPERTIES(" + join(",", tableProperties.build()) + ")";
    }

    private static void populateRowToHiveTable(String destination, List<String> values, Optional<String> partition)
    {
        String queryStatement = "INSERT INTO TABLE " + destination +
                (partition.isPresent() ? format(" PARTITION (%s) ", partition.get()) : " ") +
                "SELECT " + join(",", values) + " FROM (SELECT 'foo') x";

        onHive().executeQuery("set hive.enforce.bucketing = true");
        onHive().executeQuery("set hive.enforce.sorting = true");
        onHive().executeQuery(queryStatement);
    }

    private static void populateHivePartitionedTable(String destination, String source, String partition)
    {
        String queryStatement = format("INSERT INTO TABLE %s PARTITION (%s) SELECT * FROM %s", destination, partition, source);

        onHive().executeQuery("set hive.enforce.bucketing = true");
        onHive().executeQuery("set hive.enforce.sorting = true");
        onHive().executeQuery(queryStatement);
    }

    private static void populateHiveTable(String destination, String source)
    {
        onHive().executeQuery("set hive.enforce.bucketing = true");
        onHive().executeQuery("set hive.enforce.sorting = true");
        onHive().executeQuery(format("INSERT INTO TABLE %s SELECT * FROM %s", destination, source));
    }
}
