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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.connector.MockConnectorFactory;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.testing.DistributedQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestConnectorEventListener
{
    private final EventsBuilder generatedEvents = new EventsBuilder();

    private DistributedQueryRunner queryRunner;
    private Session session;
    private EventsAwaitingQueries queries;

    @BeforeClass
    private void setUp()
            throws Exception
    {
        session = testSessionBuilder()
                .setSystemProperty("task_concurrency", "1")
                .setCatalog("tpch")
                .setSchema("tiny")
                .setClientInfo("{\"clientVersion\":\"testVersion\"}")
                .build();
        queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                return ImmutableList.of(new MockConnectorFactory.Builder()
                        .withEventListener(new TestingEventListener(generatedEvents))
                        .build());
            }
        });
        queryRunner.createCatalog("mock-catalog", "mock");
        queries = new EventsAwaitingQueries(generatedEvents, queryRunner);
    }

    @AfterClass(alwaysRun = true)
    private void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testConnectorEventHandlerReceivingEvents()
            throws Exception
    {
        queries.runQueryAndWaitForEvents("SELECT 1", 3, session);

        assertThat(generatedEvents.getQueryCreatedEvents())
                .size().isEqualTo(1);
        assertThat(generatedEvents.getQueryCompletedEvents())
                .size().isEqualTo(1);
    }
}
