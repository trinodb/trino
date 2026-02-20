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

import io.trino.tempto.fulfillment.table.hive.HiveTableDefinition;

public final class HiveBucketedTableDefinitions
{
    public static final HiveTableDefinition BUCKETED_NATION = bucketTableDefinition("bucket_nation", false, false);

    public static final HiveTableDefinition BUCKETED_NATION_PREPARED = HiveTableDefinition.builder("bucket_nation_prepared")
            .setCreateTableDDLTemplate("Table %NAME% should be only used with CTAS queries")
            .setNoData()
            .build();

    public static final HiveTableDefinition BUCKETED_SORTED_NATION = bucketTableDefinition("bucketed_sorted_nation", true, false);

    public static final HiveTableDefinition BUCKETED_PARTITIONED_NATION = bucketTableDefinition("bucketed_partitioned_nation", false, true);

    private HiveBucketedTableDefinitions() {}

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
}
