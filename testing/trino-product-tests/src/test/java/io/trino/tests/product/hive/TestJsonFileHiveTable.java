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
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;

@ProductTest
@RequiresEnvironment(HiveStorageFormatsEnvironment.class)
@TestGroup.StorageFormatsDetailed
class TestJsonFileHiveTable
{
    /**
     * Rows with duplicate top-level keys, all in one file.
     */
    private static final String DUPLICATE_KEYS_CONTENT =
            """
            {"id":1,"a":1,"a":2,"b":"x"}
            {"id":2,"a":10,"b":"first","a":20}
            {"id":3,"a":30,"unknown":99,"a":40,"b":"y"}
            {"id":4,"b":"one","b":"two"}
            {"id":5,"a":50,"a":null}
            {"id":6,"a":60,"b":"z"}
            """;

    @Test
    void testDuplicateTopLevelKeysMatchHive(HiveStorageFormatsEnvironment env)
    {
        // Hive and Trino share a metastore, so each engine needs its own table name
        String hiveTableName = "test_json_duplicate_keys_hive";
        String trinoTableName = "test_json_duplicate_keys_trino";
        String tablePath = "/tmp/test_json_duplicate_keys";

        HdfsClient hdfsClient = env.createHdfsClient();
        hdfsClient.createDirectory(tablePath);
        // duplicate keys cannot be produced by INSERT, so the file has to be staged directly
        hdfsClient.saveFile(tablePath + "/data.json", DUPLICATE_KEYS_CONTENT);

        env.executeHiveUpdate("DROP TABLE IF EXISTS " + hiveTableName);
        env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + trinoTableName);
        try {
            // external table, so dropping it never deletes the staged file
            env.executeHiveUpdate(
                    """
                    CREATE EXTERNAL TABLE %s(id INT, a INT, b STRING)
                    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
                    STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
                    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                    LOCATION '%s'
                    """.formatted(hiveTableName, tablePath));

            // a plain SELECT runs as a fetch task, while ORDER BY would launch MapReduce
            QueryResult hiveResult = env.executeHive(format("SELECT id, a, b FROM %s", hiveTableName));

            // Hive keeps the last value of a duplicate top-level key
            assertThat(hiveResult)
                    .hasRowsCount(6)
                    .containsOnly(
                            row(1, 2, "x"),
                            row(2, 20, "first"),
                            row(3, 40, "y"),
                            row(4, null, "two"),
                            row(5, null, null),
                            row(6, 60, "z"));

            env.executeTrinoUpdate(format(
                    "CREATE TABLE hive.default.%s (id integer, a integer, b varchar) " +
                            "WITH (format = 'JSON', external_location = '%s')",
                    trinoTableName,
                    tablePath));

            assertThat(env.executeTrino(format("SELECT id, a, b FROM hive.default.%s", trinoTableName)))
                    .hasSameRowsAs(hiveResult);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS hive.default." + trinoTableName);
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + hiveTableName);
            hdfsClient.delete(tablePath);
        }
    }
}
