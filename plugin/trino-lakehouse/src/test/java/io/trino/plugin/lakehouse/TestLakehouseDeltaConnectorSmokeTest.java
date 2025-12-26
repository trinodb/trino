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

import static io.trino.plugin.deltalake.DeltaLakeTableType.DATA;
import static io.trino.plugin.deltalake.DeltaLakeTableType.HISTORY;
import static io.trino.plugin.deltalake.DeltaLakeTableType.PARTITIONS;
import static io.trino.plugin.deltalake.DeltaLakeTableType.PROPERTIES;
import static io.trino.plugin.deltalake.DeltaLakeTableType.TRANSACTIONS;
import static io.trino.plugin.lakehouse.TableType.DELTA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLakehouseDeltaConnectorSmokeTest
        extends BaseLakehouseConnectorSmokeTest
{
    protected TestLakehouseDeltaConnectorSmokeTest()
    {
        super(DELTA);
    }

    @Test
    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable)
                .hasMessage("Renaming managed tables is not allowed with current metastore configuration");
    }

    @Test
    @Override
    public void testRenameTableAcrossSchemas()
    {
        assertThatThrownBy(super::testRenameTableAcrossSchemas)
                .hasMessage("Renaming managed tables is not allowed with current metastore configuration");
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
                   location = \\E's3://test-bucket-.*/tpch/region.*'\\Q,
                   type = 'DELTA'
                )\\E""");
    }

    @Test
    void testSelectMetadataTable()
    {
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$history\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$transactions\"")).matches("VALUES (CAST(1 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$properties\"")).matches("VALUES (CAST(3 AS BIGINT))");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$partitions\"")).matches("VALUES (CAST(0 AS BIGINT))");

        // This test should get updated if a new system table is added
        assertThat(io.trino.plugin.deltalake.DeltaLakeTableType.values())
                .containsExactly(
                        DATA,
                        HISTORY,
                        TRANSACTIONS,
                        PROPERTIES,
                        PARTITIONS);

        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$files\""))
                .failure().hasMessageMatching(".* Table .* does not exist");
        assertThat(query("SELECT count(*) FROM lakehouse.tpch.\"region$timeline\""))
                .failure().hasMessageMatching(".* Table .* does not exist");
    }
}
