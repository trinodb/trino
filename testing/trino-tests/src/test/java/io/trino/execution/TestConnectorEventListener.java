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
import com.google.common.io.Closer;
import io.trino.connector.MockConnectorFactory;
import io.trino.execution.EventsCollector.EventFilters;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestConnectorEventListener
{
    private final EventsCollector generatedEvents = new EventsCollector(EventFilters.builder()
            .setSplitCompletedFilter(event -> false)
            .build());

    private Closer closer;
    private EventsAwaitingQueries queries;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        closer = Closer.create();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(TEST_SESSION).setNodeCount(1).build();
        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<ConnectorFactory> getConnectorFactories()
            {
                return ImmutableList.of(MockConnectorFactory.builder()
                        .withEventListener(new TestingEventListener(generatedEvents))
                        .build());
            }
        });
        closer.register(queryRunner);
        queryRunner.createCatalog("mock-catalog", "mock");
        queries = new EventsAwaitingQueries(generatedEvents, queryRunner, Duration.ofSeconds(1));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (closer != null) {
            closer.close();
        }
        closer = null;
    }

    @Test
    public void testConnectorEventHandlerReceivingEvents()
            throws Exception
    {
        queries.runQueryAndWaitForEvents("SELECT 1", 2, TEST_SESSION);

        List<QueryCreatedEvent> queryCreatedEvents = generatedEvents.getQueryCreatedEvents();
        List<QueryCompletedEvent> queryCompletedEvents = generatedEvents.getQueryCompletedEvents();
        List<Object> allEvents = ImmutableList.builder()
                .addAll(queryCreatedEvents)
                .addAll(queryCompletedEvents)
                .build();
        List<String> eventTypes = allEvents.stream().map(event -> event.getClass().getSimpleName()).sorted().collect(toImmutableList());
        assertThat(allEvents)
                .size().withFailMessage(() -> "got events: " + eventTypes).isEqualTo(2);
        assertThat(queryCreatedEvents)
                .size().withFailMessage(() -> "got events: " + eventTypes).isEqualTo(1);
        assertThat(queryCompletedEvents)
                .size().withFailMessage(() -> "got events: " + eventTypes).isEqualTo(1);
    }
}
