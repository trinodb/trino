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
package io.prestosql.tests.hive;

import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.stream.Stream;

import static io.prestosql.tempto.assertions.QueryAssert.Row.row;
import static io.prestosql.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tempto.query.QueryExecutor.query;
import static io.prestosql.tests.TestGroups.HIVE_TRANSACTIONAL;
import static io.prestosql.tests.TestGroups.STORAGE_FORMATS;
import static io.prestosql.tests.hive.TransactionalTableType.ACID;
import static io.prestosql.tests.hive.TransactionalTableType.INSERT_ONLY;
import static io.prestosql.tests.utils.QueryExecutors.onHive;
import static java.util.stream.Collectors.joining;

public class TestHiveTransactionalTable
        extends HiveProductTest
{
    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, dataProvider = "partitioningAndBucketingTypeDataProvider")
    public void testReadFullAcid(boolean isPartitioned, BucketingType bucketingType)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableName = "test_full_acid_table_read";
        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onHive().executeQuery("CREATE TABLE " + tableName + " (col INT, fcol INT) " +
                (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                bucketingType.getHiveClustering("fcol", 4) + " " +
                "STORED AS ORC " +
                hiveTableProperties(ACID, bucketingType));
        try {
            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " VALUES (21, 1)");

            String selectFromOnePartitionsSql = "SELECT col, fcol FROM " + tableName + " ORDER BY col";
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(21, 1));

            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " VALUES (22, 2)");
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(21, 1), row(22, 2));

            // test filtering
            assertThat(query("SELECT col, fcol FROM " + tableName + " WHERE fcol = 1 ORDER BY col")).containsOnly(row(21, 1));

            // delete a row
            onHive().executeQuery("DELETE FROM " + tableName + " WHERE fcol=2");
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(21, 1));

            // update the existing row
            String predicate = "fcol = 1" + (isPartitioned ? " AND part_col = 2 " : "");
            onHive().executeQuery("UPDATE " + tableName + " SET col = 23 WHERE " + predicate);
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(23, 1));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL}, dataProvider = "partitioningAndBucketingTypeDataProvider")
    public void testReadInsertOnly(boolean isPartitioned, BucketingType bucketingType)
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Presto Hive transactional tables are supported with Hive version 3 or above");
        }

        String tableName = "test_insert_only_table_read";
        onHive().executeQuery("DROP TABLE IF EXISTS " + tableName);
        onHive().executeQuery("CREATE TABLE " + tableName + " (col INT) " +
                (isPartitioned ? "PARTITIONED BY (part_col INT) " : "") +
                bucketingType.getHiveClustering("col", 4) + " " +
                "STORED AS ORC " +
                hiveTableProperties(INSERT_ONLY, bucketingType));
        try {
            String hivePartitionString = isPartitioned ? " PARTITION (part_col=2) " : "";
            String predicate = isPartitioned ? " WHERE part_col = 2 " : "";

            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " SELECT 1");
            String selectFromOnePartitionsSql = "SELECT col FROM " + tableName + predicate + " ORDER BY COL";
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(1));

            onHive().executeQuery("INSERT INTO TABLE " + tableName + hivePartitionString + " SELECT 2");
            assertThat(query(selectFromOnePartitionsSql)).containsExactly(row(1), row(2));

            onHive().executeQuery("INSERT OVERWRITE TABLE " + tableName + hivePartitionString + " SELECT 3");
            assertThat(query(selectFromOnePartitionsSql)).containsOnly(row(3));
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    @Test(groups = {STORAGE_FORMATS, HIVE_TRANSACTIONAL})
    public void testFailAcidBeforeHive3()
    {
        if (getHiveVersionMajor() >= 3) {
            throw new SkipException("This tests behavior of ACID table before Hive 3 ");
        }

        String tableName = "test_fail_acid_before_hive_3";
        onHive().executeQuery("" +
                "CREATE TABLE " + tableName + "(a bigint) " +
                "CLUSTERED BY(a) INTO 4 BUCKETS " +
                "STORED AS ORC " +
                "TBLPROPERTIES ('transactional'='true')");
        try {
            assertThat(() -> query("SELECT * FROM " + tableName))
                    .failsWithMessage("Failed to open transaction. Transactional tables support requires Hive metastore version at least 3.0");
        }
        finally {
            onHive().executeQuery("DROP TABLE " + tableName);
        }
    }

    @DataProvider
    public Object[][] partitioningAndBucketingTypeDataProvider()
    {
        return Stream.of(BucketingType.values())
                .flatMap(value -> Stream.of(
                        new Object[] {false, value},
                        new Object[] {true, value}))
                .toArray(Object[][]::new);
    }

    private static String hiveTableProperties(TransactionalTableType transactionalTableType, BucketingType bucketingType)
    {
        return Stream.concat(
                transactionalTableType.getHiveTableProperties().stream(),
                bucketingType.getHiveTableProperties().stream())
                .collect(joining(",", "TBLPROPERTIES (", ")"));
    }
}
