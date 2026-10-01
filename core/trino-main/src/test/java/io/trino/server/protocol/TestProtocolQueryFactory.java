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
package io.trino.server.protocol;

import com.google.common.collect.ImmutableList;
import com.google.inject.Key;
import io.airlift.units.Duration;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.client.QueryData;
import io.trino.client.QueryResults;
import io.trino.dispatcher.DispatchManager;
import io.trino.server.ExternalUriInfo;
import io.trino.server.SessionContext;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.Page;
import io.trino.spi.QueryId;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestProtocolQueryFactory
{
    private static final Duration WAIT = new Duration(1, SECONDS);

    @Test
    public void testDrainQueryWithCustomQueryDataProducer()
            throws Exception
    {
        try (TestingTrinoServer server = TestingTrinoServer.create()) {
            DispatchManager dispatchManager = server.getDispatchManager();
            Session session = testSessionBuilder()
                    .setCatalog(Optional.empty())
                    .setSchema(Optional.empty())
                    .build();
            QueryId queryId = dispatchManager.createQueryId();
            Slug slug = Slug.createNew();
            getFutureValue(dispatchManager.createQuery(
                    queryId,
                    Span.getInvalid(),
                    slug,
                    SessionContext.fromSession(session),
                    "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name)"));
            getFutureValue(dispatchManager.waitForDispatched(queryId));

            RecordingQueryDataProducerFactory queryDataProducerFactory = new RecordingQueryDataProducerFactory();
            ProtocolQuery query = server.getInstance(Key.get(ProtocolQueryFactory.class))
                    .create(server.getQueryManager().getQuerySession(queryId), slug, queryDataProducerFactory);
            ExternalUriInfo externalUriInfo = ExternalUriInfo.forBaseUri(server.getBaseUrl());

            try {
                long token = 0;
                QueryResults results;
                do {
                    results = getFutureValue(query.waitForResults(token, externalUriInfo, WAIT)).queryResults();
                    assertThat(results.getError()).isNull();
                    token++;
                }
                while (results.getNextUri() != null);
                query.markResultsConsumedIfReady();
            }
            finally {
                query.dispose();
            }

            assertThat(queryDataProducerFactory.columnNames()).containsExactly("id", "name");
            assertThat(queryDataProducerFactory.columnTypes()).containsExactly(INTEGER, createVarcharType(1));
            assertThat(queryDataProducerFactory.rows()).containsExactly(
                    ImmutableList.of(1, "a"),
                    ImmutableList.of(2, "b"));
        }
    }

    /**
     * Captures what the query hands to its producer instead of encoding it, so the test can see
     * the columns and rows that a caller-supplied encoding would receive.
     */
    private static class RecordingQueryDataProducerFactory
            implements QueryDataProducerFactory
    {
        // Written on the query's result processing threads, read by the test thread
        private final List<String> columnNames = new CopyOnWriteArrayList<>();
        private final List<Type> columnTypes = new CopyOnWriteArrayList<>();
        private final List<List<Object>> rows = new CopyOnWriteArrayList<>();

        @Override
        public QueryDataProducer create(Session session, List<String> columnNames, List<Type> columnTypes)
        {
            this.columnNames.addAll(columnNames);
            this.columnTypes.addAll(columnTypes);
            return (_, queryResultRows, _) -> {
                for (Page page : queryResultRows.getPages()) {
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        ImmutableList.Builder<Object> row = ImmutableList.builder();
                        for (int channel = 0; channel < page.getChannelCount(); channel++) {
                            row.add(columnTypes.get(channel).getObjectValue(page.getBlock(channel), position));
                        }
                        rows.add(row.build());
                    }
                }
                return QueryData.NULL;
            };
        }

        public List<String> columnNames()
        {
            return ImmutableList.copyOf(columnNames);
        }

        public List<Type> columnTypes()
        {
            return ImmutableList.copyOf(columnTypes);
        }

        public List<List<Object>> rows()
        {
            return ImmutableList.copyOf(rows);
        }
    }
}
