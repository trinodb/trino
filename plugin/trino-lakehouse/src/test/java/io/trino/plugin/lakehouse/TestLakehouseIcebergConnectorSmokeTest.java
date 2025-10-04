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

import static io.trino.plugin.lakehouse.TableType.ICEBERG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat((Long) computeScalar("SELECT count(*) FROM lakehouse.tpch.\"region$history\"")).isEqualTo(1L);
        assertThat((Long) computeScalar("SELECT count(*) FROM lakehouse.tpch.\"region$metadata_log_entries\"")).isEqualTo(1L);
        assertThat((Long) computeScalar("SELECT count(*) FROM lakehouse.tpch.\"region$snapshots\"")).isEqualTo(1L);
        assertThat((Long) computeScalar("SELECT count(*) FROM lakehouse.tpch.\"region$all_manifests\"")).isEqualTo(1L);
        assertThat((Long) computeScalar("SELECT count(*) FROM lakehouse.tpch.\"region$manifests\"")).isEqualTo(1L);
        assertThat((Long) computeScalar("SELECT count(*) FROM lakehouse.tpch.\"region$partitions\"")).isEqualTo(1L);
        assertThat((Long) computeScalar("SELECT count(*) FROM lakehouse.tpch.\"region$files\"")).isEqualTo(1L);
        assertThat((Long) computeScalar("SELECT count(*) FROM lakehouse.tpch.\"region$all_entries\"")).isEqualTo(1L);
        assertThat((Long) computeScalar("SELECT count(*) FROM lakehouse.tpch.\"region$entries\"")).isEqualTo(1L);
        assertThat((Long) computeScalar("SELECT count(*) FROM lakehouse.tpch.\"region$properties\"")).isEqualTo(6L);
        assertThat((Long) computeScalar("SELECT count(*) FROM lakehouse.tpch.\"region$refs\"")).isEqualTo(1L);

        assertThatThrownBy(() -> computeScalar("SELECT count(*) FROM lakehouse.tpch.\"region$timeline\""))
                .hasMessageMatching(".* Table .* does not exist");
    }
}
