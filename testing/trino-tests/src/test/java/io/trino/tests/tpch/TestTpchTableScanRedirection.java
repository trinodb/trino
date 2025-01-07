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
package io.trino.tests.tpch;

import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTpchTableScanRedirection
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = TpchQueryRunner.builder()
                .withConnectorProperties(Map.of(
                        "tpch.table-scan-redirection-catalog", "memory",
                        "tpch.table-scan-redirection-schema", "test"))
                .build();
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory");
        // Add another tpch catalog without redirection to aid in loading data into memory connector
        queryRunner.createCatalog("tpch_data_load", "tpch");
        queryRunner.execute("CREATE SCHEMA memory.test");
        return queryRunner;
    }

    @Test
    @Timeout(20)
    public void testTableScanRedirection()
    {
        // select orderstatus, count(*) from tpch.tiny.orders group by 1
        // O           |  7333
        // P           |   363
        // F           |  7304
        assertUpdate("CREATE TABLE memory.test.orders AS SELECT * FROM tpch_data_load.tiny.orders WHERE orderstatus IN ('O', 'P')", 7696L);
        // row count of 7333L verifies that filter was coorectly re-materialized during redirection and that redirection has taken place
        assertThat(computeActual("SELECT * FROM tpch.tiny.orders WHERE orderstatus IN ('O', 'F')").getRowCount()).isEqualTo(7333L);
    }

    @Test
    @Timeout(20)
    public void testTableScanRedirectionWithCoercion()
    {
        assertUpdate("CREATE TABLE memory.test.nation AS SELECT * FROM (VALUES '42') t(nationkey)", 1L);
        assertQuery("SELECT nationkey FROM tpch.tiny.nation", "VALUES 42");
    }
}
