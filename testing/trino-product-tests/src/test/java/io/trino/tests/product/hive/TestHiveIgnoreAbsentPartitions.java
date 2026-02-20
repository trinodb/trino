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

import io.trino.testing.containers.HdfsClient;
import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for Hive ignore_absent_partitions session property.
 * <p>
 * Ported from the Tempto-based TestHiveIgnoreAbsentPartitions.
 */
@ProductTest
@RequiresEnvironment(HiveBasicEnvironment.class)
@TestGroup.HdfsNoImpersonation
class TestHiveIgnoreAbsentPartitions
{
    private static final Pattern ACID_LOCATION_PATTERN = Pattern.compile("(.*)/delta_[^/]+");

    @Test
    void testIgnoreAbsentPartitions(HiveBasicEnvironment env)
    {
        String tableName = "test_ignore_absent_partitions";
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        env.executeTrinoUpdate(format(
                "CREATE TABLE hive.default.%s (n_nationkey BIGINT, n_name VARCHAR, n_comment VARCHAR, p_regionkey BIGINT) " +
                        "WITH (format = 'ORC', partitioned_by = ARRAY['p_regionkey'])",
                tableName));

        env.executeTrinoUpdate(format(
                "INSERT INTO hive.default.%s " +
                        "SELECT n.nationkey, n.name, n.comment, n.regionkey " +
                        "FROM tpch.tiny.nation n " +
                        "WHERE n.regionkey < 3",
                tableName));

        String tablePath = getTablePath(env, "hive.default." + tableName, 1);
        String partitionPath = format("%s/p_regionkey=9999", tablePath);

        HdfsClient hdfsClient = env.createHdfsClient();

        assertThat(env.executeTrino("SELECT count(*) FROM hive.default." + tableName)).containsOnly(row(15L));

        // Verify the partition doesn't exist yet
        assertThat(hdfsClient.exist(partitionPath))
                .as(format("Expected partition %s to not exist", partitionPath))
                .isFalse();

        // Create an empty partition using the procedure
        env.executeTrinoUpdate(format(
                "CALL hive.system.create_empty_partition('default', '%s', array['p_regionkey'], array['9999'])",
                tableName));

        // Set session to NOT ignore absent partitions
        env.executeTrinoUpdate("SET SESSION hive.ignore_absent_partitions = false");

        // Delete the partition directory from HDFS
        hdfsClient.delete(partitionPath);
        assertThat(hdfsClient.exist(partitionPath))
                .as(format("Expected partition %s to not exist after deletion", partitionPath))
                .isFalse();

        // Query should fail because partition location does not exist
        assertThatThrownBy(() -> env.executeTrino("SELECT count(*) FROM hive.default." + tableName))
                .hasMessageContaining("Partition location does not exist");

        // Set session to ignore absent partitions
        env.executeTrinoUpdate("SET SESSION hive.ignore_absent_partitions = true");

        // Query should now succeed, returning only rows from existing partitions
        assertThat(env.executeTrino("SELECT count(*) FROM hive.default." + tableName)).containsOnly(row(15L));

        // Cleanup
        env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
    }

    @Test
    void testShouldThrowErrorOnUnpartitionedTableMissingData(HiveBasicEnvironment env)
    {
        String tableName = "unpartitioned_absent_table_data";

        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + tableName);

        // Create table with some data
        assertThat(env.executeTrino(format(
                "CREATE TABLE hive.default.%s AS SELECT * FROM (VALUES 1,2,3) t(dummy_col)",
                tableName))).containsOnly(row(3L));
        assertThat(env.executeTrino("SELECT count(*) FROM hive.default." + tableName)).containsOnly(row(3L));

        String tablePath = getTablePath(env, "hive.default." + tableName, 0);
        HdfsClient hdfsClient = env.createHdfsClient();

        assertThat(hdfsClient.exist(tablePath)).isTrue();
        hdfsClient.delete(tablePath);

        // With ignore_absent_partitions = false, query should fail
        env.executeTrinoUpdate("SET SESSION hive.ignore_absent_partitions = false");
        assertThatThrownBy(() -> env.executeTrino("SELECT count(*) FROM hive.default." + tableName))
                .hasMessageContaining("Partition location does not exist");

        // With ignore_absent_partitions = true, query should STILL fail for unpartitioned tables
        env.executeTrinoUpdate("SET SESSION hive.ignore_absent_partitions = true");
        assertThatThrownBy(() -> env.executeTrino("SELECT count(*) FROM hive.default." + tableName))
                .hasMessageContaining("Partition location does not exist");

        env.executeTrinoUpdate("DROP TABLE hive.default." + tableName);
    }

    private String getTableLocation(HiveBasicEnvironment env, String tableName, int partitionColumns)
    {
        StringBuilder regex = new StringBuilder("/[^/]*$");
        for (int i = 0; i < partitionColumns; i++) {
            regex.insert(0, "/[^/]*");
        }
        String tableLocation = (String) env.executeTrino(
                        format("SELECT DISTINCT regexp_replace(\"$path\", '%s', '') FROM %s", regex, tableName))
                .getOnlyValue();

        // trim the /delta_... suffix for ACID tables
        Matcher acidLocationMatcher = ACID_LOCATION_PATTERN.matcher(tableLocation);
        if (acidLocationMatcher.matches()) {
            tableLocation = acidLocationMatcher.group(1);
        }
        return tableLocation;
    }

    private String getTablePath(HiveBasicEnvironment env, String tableName, int partitionColumns)
    {
        String location = getTableLocation(env, tableName, partitionColumns);
        return URI.create(location).getPath();
    }
}
