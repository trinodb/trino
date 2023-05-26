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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.jdbc.H2QueryRunner.createH2QueryRunner;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

// Single-threaded because of shared mutable state, e.g. onGetTableProperties
@Test(singleThreaded = true)
public class TestJdbcTableProperties
        extends AbstractTestQueryFramework
{
    private final Map<String, String> properties = TestingH2JdbcModule.createProperties();
    private Runnable onGetTableProperties = () -> {};

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingH2JdbcModule module = new TestingH2JdbcModule((config, connectionFactory, identifierMapping) -> new TestingH2JdbcClient(config, connectionFactory, identifierMapping)
        {
            @Override
            public Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle)
            {
                onGetTableProperties.run();
                return ImmutableMap.of();
            }
        });
        return createH2QueryRunner(ImmutableList.copyOf(TpchTable.getTables()), properties, module);
    }

    @BeforeMethod
    public void reset()
    {
        onGetTableProperties = () -> {};
    }

    @Test
    public void testGetTablePropertiesIsNotCalledForSelect()
    {
        onGetTableProperties = () -> { fail("Unexpected call of: getTableProperties"); };
        assertUpdate("CREATE TABLE copy_of_nation AS SELECT * FROM nation", 25);
        assertQuerySucceeds("SELECT * FROM copy_of_nation");
        assertQuerySucceeds("SELECT nationkey FROM copy_of_nation");
    }

    @Test
    public void testGetTablePropertiesIsCalled()
    {
        AtomicInteger counter = new AtomicInteger();
        onGetTableProperties = () -> { counter.incrementAndGet(); };
        assertQuerySucceeds("SHOW CREATE TABLE nation");
        assertThat(counter.get()).isOne();
    }
}
