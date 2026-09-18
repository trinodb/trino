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
package io.trino.plugin.lakehouse;

import org.junit.jupiter.api.Test;

import static io.trino.plugin.iceberg.TableType.ALL_ENTRIES;
import static io.trino.plugin.iceberg.TableType.ALL_MANIFESTS;
import static io.trino.plugin.iceberg.TableType.DATA;
import static io.trino.plugin.iceberg.TableType.ENTRIES;
import static io.trino.plugin.iceberg.TableType.FILES;
import static io.trino.plugin.iceberg.TableType.HISTORY;
import static io.trino.plugin.iceberg.TableType.MANIFESTS;
import static io.trino.plugin.iceberg.TableType.MATERIALIZED_VIEW_STORAGE;
import static io.trino.plugin.iceberg.TableType.METADATA_LOG_ENTRIES;
import static io.trino.plugin.iceberg.TableType.PARTITIONS;
import static io.trino.plugin.iceberg.TableType.PROPERTIES;
import static io.trino.plugin.iceberg.TableType.REFS;
import static io.trino.plugin.iceberg.TableType.SNAPSHOTS;
import static io.trino.plugin.lakehouse.TableType.ICEBERG;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLakehouseIcebergConnectorSmokeTest
        extends BaseLakehouseConnectorSmokeTest
{
    protected TestLakehouseIcebergConnectorSmokeTest()
    {
        super(ICEBERG);
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region")).matches(
                """
                \\QCREATE TABLE lakehouse.tpch.region (
                   regionkey bigint,
                   name varchar,
                   comment varchar
                )
                WITH (
                   format = 'PARQUET',
                   format_version = 2,
                   location = \\E's3://test-bucket-.*/tpch/region-.*'\\Q,
                   type = 'ICEBERG'
                )\\E""");
    }

    @Test
    void testSelectMetadataTable()
    {
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$history\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$metadata_log_entries\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$snapshots\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$all_manifests\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$manifests\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$partitions\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$files\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$all_entries\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$entries\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$properties\"")).matches("VALUES (CAST(6 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$refs\"")).matches("VALUES (CAST(1 AS BIGINT))");

        // This test should get updated if a new system table is added
        assertThat(io.trino.plugin.iceberg.TableType.values())
                .containsExactly(
                        DATA,
                        HISTORY,
                        METADATA_LOG_ENTRIES,
                        SNAPSHOTS,
                        ALL_MANIFESTS,
                        MANIFESTS,
                        PARTITIONS,
                        FILES,
                        ALL_ENTRIES,
                        ENTRIES,
                        PROPERTIES,
                        REFS,
                        MATERIALIZED_VIEW_STORAGE);

        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$timeline\""))
                .failure().hasMessageMatching(".* Table .* does not exist");
    }
}
