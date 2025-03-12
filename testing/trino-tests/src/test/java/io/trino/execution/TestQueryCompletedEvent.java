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
import io.trino.dispatcher.DispatchQuery;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.spi.Plugin;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.testing.StandaloneQueryRunner;
import io.trino.testing.TestingDirectTrinoClient;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static io.trino.execution.QueryState.FINISHED;
import static io.trino.testing.TestingSession.testSession;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.Collections.synchronizedSet;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryCompletedEvent
{
    @Test
    public void testQueryCompletedEventIssued()
    {
        try (StandaloneQueryRunner queryRunner = new StandaloneQueryRunner(testSession())) {
            queryRunner.installPlugin(new MemoryPlugin());
            queryRunner.createCatalog("memory", "memory");
            Set<String> queryCompletedQueryIds = synchronizedSet(new HashSet<>());
            EventListener listener = new EventListener()
            {
                @Override
                public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
                {
                    queryCompletedQueryIds.add(queryCompletedEvent.getMetadata().getQueryId());
                }
            };
            queryRunner.installPlugin(new Plugin()
            {
                @Override
                public Iterable<EventListenerFactory> getEventListenerFactories()
                {
                    return ImmutableList.of(new EventListenerFactory()
                    {
                        @Override
                        public String getName()
                        {
                            return "testQueryCompletedEventIssued";
                        }

                        @Override
                        public EventListener create(Map<String, String> config, EventListenerContext context)
                        {
                            return listener;
                        }
                    });
                }
            });

            assertQueryCompletedIssued(queryRunner, queryCompletedQueryIds, "SELECT 1");
            assertQueryCompletedIssued(queryRunner, queryCompletedQueryIds, "CREATE SCHEMA memory.test_schema");
            assertQueryCompletedIssued(queryRunner, queryCompletedQueryIds, "DROP SCHEMA memory.test_schema");
        }
    }

    private static void assertQueryCompletedIssued(StandaloneQueryRunner queryRunner, Set<String> queryCompletedQueryIds, String sql)
    {
        TestingDirectTrinoClient.Result result = queryRunner.executeWithoutResults(testSession(), sql);
        DispatchQuery query = queryRunner.getCoordinator().getDispatchManager().getQuery(result.queryId());
        assertThat(query.getState()).isEqualTo(FINISHED);
        assertEventually(() -> assertThat(queryCompletedQueryIds)
                .describedAs("query '%s'", sql)
                .contains(result.queryId().getId()));
    }
}
