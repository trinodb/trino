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
package io.prestosql.tests;

import io.prestosql.Session;
import io.prestosql.plugin.memory.MemoryPlugin;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

import static io.prestosql.SystemSessionProperties.TABLE_SCAN_REDIRECTION_ENABLED;
import static org.testng.Assert.assertEquals;

public class TestTpchTableScanRedirection
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = TpchQueryRunnerBuilder.builder()
                .withTableScanRedirectionCatalog("memory")
                .withTableScanRedirectionSchema("test")
                .build();
        queryRunner.installPlugin(new MemoryPlugin());
        queryRunner.createCatalog("memory", "memory");
        return queryRunner;
    }

    @Test(timeOut = 20_000)
    public void testTableScanRedirection()
    {
        assertQuerySucceeds("CREATE SCHEMA memory.test");
        assertUpdate(
                Session.builder(getSession())
                        .setSystemProperty(TABLE_SCAN_REDIRECTION_ENABLED, "false")
                        .build(),
                "CREATE TABLE memory.test.orders AS SELECT * FROM tpch.tiny.orders LIMIT 100", 100L);
        assertEquals(computeActual("SELECT * FROM tpch.tiny.orders WHERE orderkey > 0").getRowCount(), 100L);
    }
}
