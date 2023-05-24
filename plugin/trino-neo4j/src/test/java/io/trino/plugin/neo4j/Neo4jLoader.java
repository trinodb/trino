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
package io.trino.plugin.neo4j;

import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.QueryData;
import io.trino.client.QueryStatusInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.AbstractTestingTrinoClient;
import io.trino.testing.ResultsSession;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

public class Neo4jLoader
        extends AbstractTestingTrinoClient<Void>
{
    private final String tableName;
    private final TestingNeo4jServer neo4jServer;

    public Neo4jLoader(TestingNeo4jServer neo4jServer, String tableName,
            TestingTrinoServer trinoServer,
            Session defaultSession)
    {
        super(trinoServer, defaultSession);
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.neo4jServer = neo4jServer;
    }

    @Override
    protected ResultsSession<Void> getResultSession(Session session)
    {
        requireNonNull(session, "session is null");
        return new Neo4jLoadingSession();
    }

    private class Neo4jLoadingSession
            implements ResultsSession<Void>
    {
        private final AtomicReference<List<Type>> types = new AtomicReference<>();
        private final AtomicReference<List<String>> names = new AtomicReference<>();

        private Neo4jLoadingSession() {}

        @Override
        public void addResults(QueryStatusInfo statusInfo, QueryData data)
        {
            if (types.get() == null && statusInfo.getColumns() != null) {
                types.set(getTypes(statusInfo.getColumns()));
            }
            if (names.get() == null && statusInfo.getColumns() != null) {
                names.set(getNames(statusInfo.getColumns()));
            }

            if (data.getData() == null) {
                return;
            }
            checkState(types.get() != null, "Type information is missing");
            checkState(names.get() != null, "Column names are missing");
            List<Column> columns = statusInfo.getColumns();

            String query = String.format("UNWIND $props AS properties " +
                    "CREATE (n:%s) " +
                    "SET n = properties", tableName);
            List<Map<String, Object>> propsList = new ArrayList<>();
            for (List<Object> fields : data.getData()) {
                Map<String, Object> propsMap = new HashMap<>();
                for (int i = 0; i < fields.size(); i++) {
                    Type type = types.get().get(i);
                    Object value = convertValue(fields.get(i), type);
                    propsMap.put(names.get().get(i), value);
                }
                propsList.add(propsMap);
            }
            Map<String, Object> parameters = Collections.singletonMap("props", propsList);
            try (Driver driver = GraphDatabase.driver(neo4jServer.getBoltUrl(), AuthTokens.basic(neo4jServer.getUsername(), neo4jServer.getPassword()));
                    org.neo4j.driver.Session session = driver.session();) {
                session.writeTransaction(tx -> tx.run(query, parameters));
            }
        }

        private Object convertValue(Object value, Type type)
        {
            if (value == null) {
                return null;
            }

            if (type == DATE && value instanceof String) {
                return LocalDate.parse((String) value);
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

        @Override
        public Void build(Map<String, String> setSessionProperties, Set<String> resetSessionProperties)
        {
            return null;
        }
    }
}
