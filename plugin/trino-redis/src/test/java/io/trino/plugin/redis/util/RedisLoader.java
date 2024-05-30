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
package io.trino.plugin.redis.util;

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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

public class RedisLoader
        extends AbstractTestingTrinoClient<Void>
{
    private final JedisPool jedisPool;
    private final String tableName;
    private final String dataFormat;
    private final AtomicLong count = new AtomicLong();
    private final JsonEncoder jsonEncoder;

    public RedisLoader(
            TestingTrinoServer trinoServer,
            Session defaultSession,
            JedisPool jedisPool,
            String tableName,
            String dataFormat)
    {
        super(trinoServer, defaultSession);
        this.jedisPool = jedisPool;
        this.tableName = tableName;
        this.dataFormat = dataFormat;
        jsonEncoder = new JsonEncoder();
    }

    @Override
    public ResultsSession<Void> getResultSession(Session session)
    {
        requireNonNull(session, "session is null");
        return new RedisLoadingSession();
    }

    private class RedisLoadingSession
            implements ResultsSession<Void>
    {
        private final AtomicReference<List<Type>> types = new AtomicReference<>();

        @Override
        public void addResults(QueryStatusInfo statusInfo, QueryData data)
        {
            if (types.get() == null && statusInfo.getColumns() != null) {
                types.set(getTypes(statusInfo.getColumns()));
            }

            if (data.getData() != null) {
                checkState(types.get() != null, "Data without types received!");
                List<Column> columns = statusInfo.getColumns();
                for (List<Object> fields : data.getData()) {
                    String redisKey = tableName + ":" + count.getAndIncrement();

                    try (Jedis jedis = jedisPool.getResource()) {
                        switch (dataFormat) {
                            case "string":
                                ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
                                for (int i = 0; i < fields.size(); i++) {
                                    Type type = types.get().get(i);
                                    Object value = convertValue(fields.get(i), type);
                                    if (value != null) {
                                        builder.put(columns.get(i).getName(), value);
                                    }
                                }
                                jedis.set(redisKey, jsonEncoder.toString(builder.buildOrThrow()));
                                break;
                            case "hash":
                                // add keys to zset
                                String redisZset = "keyset:" + tableName;
                                jedis.zadd(redisZset, count.get(), redisKey);
                                // add values to Hash
                                for (int i = 0; i < fields.size(); i++) {
                                    jedis.hset(redisKey, columns.get(i).getName(), fields.get(i).toString());
                                }
                                break;
                            default:
                                throw new AssertionError("unhandled value type: " + dataFormat);
                        }
                    }
                }
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

            if (BOOLEAN.equals(type) || type instanceof VarcharType) {
                return value;
            }
            if (BIGINT.equals(type)) {
                return ((Number) value).longValue();
            }
            if (INTEGER.equals(type)) {
                return ((Number) value).intValue();
            }
            if (DOUBLE.equals(type)) {
                return ((Number) value).doubleValue();
            }
            if (DATE.equals(type)) {
                return value;
            }
            throw new AssertionError("unhandled type: " + type);
        }
    }
}
