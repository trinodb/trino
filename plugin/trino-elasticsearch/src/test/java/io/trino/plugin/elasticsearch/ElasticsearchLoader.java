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
package io.trino.plugin.elasticsearch;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.json.JsonMapperProvider;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.QueryStatusInfo;
import io.trino.client.ResultRows;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.AbstractTestingTrinoClient;
import io.trino.testing.ResultsSession;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

public class ElasticsearchLoader
        extends AbstractTestingTrinoClient<Void>
{
    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider().get();

    private final String tableName;
    private final RestClient client;

    public ElasticsearchLoader(
            RestClient client,
            String tableName,
            TestingTrinoServer trinoServer,
            Session defaultSession)
    {
        super(trinoServer, defaultSession);

        this.tableName = requireNonNull(tableName, "tableName is null");
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public ResultsSession<Void> getResultSession(Session session)
    {
        requireNonNull(session, "session is null");
        return new ElasticsearchLoadingSession();
    }

    private class ElasticsearchLoadingSession
            implements ResultsSession<Void>
    {
        private final AtomicReference<List<Type>> types = new AtomicReference<>();

        private ElasticsearchLoadingSession() {}

        @Override
        public void addResults(QueryStatusInfo statusInfo, ResultRows data)
        {
            if (types.get() == null && statusInfo.getColumns() != null) {
                types.set(getTypes(statusInfo.getColumns()));
            }

            if (data.isNull()) {
                return;
            }
            checkState(types.get() != null, "Type information is missing");
            List<Column> columns = statusInfo.getColumns();

            StringBuilder bulkBody = new StringBuilder();
            for (List<Object> fields : data) {
                try {
                    // Action line
                    ObjectNode action = JSON_MAPPER.createObjectNode();
                    ObjectNode indexAction = JSON_MAPPER.createObjectNode();
                    indexAction.put("_index", tableName);
                    action.set("index", indexAction);
                    bulkBody.append(JSON_MAPPER.writeValueAsString(action)).append('\n');

                    // Data line
                    ObjectNode doc = JSON_MAPPER.createObjectNode();
                    for (int i = 0; i < fields.size(); i++) {
                        Type type = types.get().get(i);
                        Object value = convertValue(fields.get(i), type);
                        String columnName = columns.get(i).getName();
                        if (value == null) {
                            doc.putNull(columnName);
                        }
                        else if (value instanceof Boolean b) {
                            doc.put(columnName, b);
                        }
                        else if (value instanceof Long l) {
                            doc.put(columnName, l);
                        }
                        else if (value instanceof Integer n) {
                            doc.put(columnName, n);
                        }
                        else if (value instanceof Double d) {
                            doc.put(columnName, d);
                        }
                        else if (value instanceof String s) {
                            doc.put(columnName, s);
                        }
                        else {
                            doc.put(columnName, value.toString());
                        }
                    }
                    bulkBody.append(JSON_MAPPER.writeValueAsString(doc)).append('\n');
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Error loading data into Elasticsearch index: " + tableName, e);
                }
            }

            try {
                Request request = new Request("POST", "/_bulk");
                request.addParameter("refresh", "true");
                request.setEntity(new StringEntity(bulkBody.toString(), ContentType.create("application/x-ndjson")));
                client.performRequest(request);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Void build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties)
        {
            return null;
        }

        private Object convertValue(Object value, Type type)
        {
            if (value == null) {
                return null;
            }

            if (type == BOOLEAN || type == DATE || type instanceof VarcharType) {
                return value;
            }
            if (type == BIGINT) {
                return ((Number) value).longValue();
            }
            if (type == INTEGER) {
                return ((Number) value).intValue();
            }
            if (type == DOUBLE) {
                return ((Number) value).doubleValue();
            }
            throw new IllegalArgumentException("Unhandled type: " + type);
        }
    }
}
