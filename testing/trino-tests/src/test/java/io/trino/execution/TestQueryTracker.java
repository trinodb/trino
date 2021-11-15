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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

import static io.trino.SystemSessionProperties.QUERY_MAX_PLANNING_TIME;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Tests need to finish before strict timeouts. Any background work
// may make them flaky
@Test(singleThreaded = true)
public class TestQueryTracker
        extends AbstractTestQueryFramework
{
    private final CountDownLatch freeze = new CountDownLatch(1);
    private final CountDownLatch interrupted = new CountDownLatch(1);

    @AfterClass
    public void unfreeze()
    {
        freeze.countDown();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("mock")
                .setSchema("default")
                .setSystemProperty(QUERY_MAX_PLANNING_TIME, "1s")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner
                .builder(defaultSession)
                .build();
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                return ImmutableList.of(MockConnectorFactory.builder()
                        .withGetColumns(ignored -> ImmutableList.of(new ColumnMetadata("col", VARCHAR)))
                        // Apply filter happens inside optimizer so this should model most blocking tasks in planning phase
                        .withApplyFilter((ignored1, ignored2, ignored3) -> freeze())
                        .build());
            }
        });
        queryRunner.createCatalog("mock", "mock");

        return queryRunner;
    }

    @Test(timeOut = 5_000)
    public void testInterruptApplyFilter()
            throws InterruptedException
    {
        assertThatThrownBy(() -> getQueryRunner().execute("SELECT * FROM t1 WHERE col = 'abc'"))
                .hasMessageContaining("Query exceeded the maximum planning time limit of 1.00s");

        interrupted.await();
    }

    private <T> T freeze()
    {
        try {
            freeze.await();
        }
        catch (InterruptedException e) {
            interrupted.countDown();
            throw new RuntimeException(e);
        }

        return null;
    }
}
