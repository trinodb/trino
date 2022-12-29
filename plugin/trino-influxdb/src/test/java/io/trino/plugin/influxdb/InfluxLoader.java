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
package io.trino.plugin.influxdb;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.client.Column;
import io.trino.client.QueryData;
import io.trino.client.QueryStatusInfo;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.AbstractTestingTrinoClient;
import io.trino.testing.ResultsSession;
import org.influxdb.dto.Point;

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
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class InfluxLoader
        extends AbstractTestingTrinoClient<Void>
{
    private final String schemaName;
    private final String tableName;
    private final InfluxSession session;

    public InfluxLoader(
            InfluxSession session,
            String schemaName,
            String tableName,
            TestingTrinoServer trinoServer,
            Session defaultSession)
    {
        super(trinoServer, defaultSession);

        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public ResultsSession<Void> getResultSession(Session session)
    {
        requireNonNull(session, "session is null");
        return new InfluxLoadingSession();
    }

    private class InfluxLoadingSession
            implements ResultsSession<Void>
    {
        private final AtomicReference<List<Type>> types = new AtomicReference<>();

        private InfluxLoadingSession() {}

        @Override
        public void addResults(QueryStatusInfo statusInfo, QueryData data)
        {
            if (types.get() == null && statusInfo.getColumns() != null) {
                types.set(getTypes(statusInfo.getColumns()));
            }

            if (data.getData() == null) {
                return;
            }
            checkState(types.get() != null, "Type information is missing");
            List<Column> columns = statusInfo.getColumns();

            for (List<Object> fields : data.getData()) {
                ImmutableMap.Builder<String, Object> fieldBuilder = ImmutableMap.builderWithExpectedSize(fields.size());
                Point.Builder pointBuilder = Point.measurement(tableName).time(System.currentTimeMillis(), MILLISECONDS);
                for (int i = 0; i < fields.size(); i++) {
                    Type type = types.get().get(i);
                    String name = columns.get(i).getName();
                    Object value = convertValue(fields.get(i), type);
                    fieldBuilder.put(name, value);
                }
                pointBuilder.fields(fieldBuilder.buildOrThrow());
                session.write(schemaName, pointBuilder.build());
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
            if (type instanceof VarcharType || type == BOOLEAN || type == DATE) {
                return value;
            }
            if (type == INTEGER) {
                return ((Number) value).intValue();
            }
            if (type == BIGINT) {
                return ((Number) value).longValue();
            }
            if (type == DOUBLE) {
                return ((Number) value).doubleValue();
            }
            throw new IllegalArgumentException("Unhandled type: " + type);
        }
    }
}
