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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import io.airlift.json.JsonCodec;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.redis.RedisTableDescription;
import io.trino.plugin.redis.TestingRedisPlugin;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingTrinoClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;

public final class RedisTestUtils
{
    private RedisTestUtils() {}

    public static void installRedisPlugin(RedisServer redisServer, QueryRunner queryRunner, Map<SchemaTableName, RedisTableDescription> tableDescriptions, Map<String, String> connectorProperties)
    {
        queryRunner.installPlugin(new TestingRedisPlugin(tableDescriptions));

        // note: additional copy via ImmutableList so that if fails on nulls
        connectorProperties = new HashMap<>(ImmutableMap.copyOf(connectorProperties));
        connectorProperties.putIfAbsent("redis.nodes", redisServer.getHostAndPort().toString());
        connectorProperties.putIfAbsent("redis.table-names", Joiner.on(",").join(tableDescriptions.keySet()));
        connectorProperties.putIfAbsent("redis.default-schema", "default");
        connectorProperties.putIfAbsent("redis.hide-internal-columns", "true");
        connectorProperties.putIfAbsent("redis.key-prefix-schema-table", "true");

        queryRunner.createCatalog("redis", "redis", connectorProperties);
    }

    public static void loadTpchTable(RedisServer redisServer, TestingTrinoClient trinoClient, String tableName, QualifiedObjectName tpchTableName, String dataFormat)
    {
        RedisLoader tpchLoader = new RedisLoader(trinoClient.getServer(), trinoClient.getDefaultSession(), redisServer.getJedisPool(), tableName, dataFormat);
        tpchLoader.execute(format("SELECT * from %s", tpchTableName));
    }

    public static Map.Entry<SchemaTableName, RedisTableDescription> loadTpchTableDescription(
            JsonCodec<RedisTableDescription> tableDescriptionJsonCodec,
            SchemaTableName schemaTableName,
            String dataFormat)
            throws IOException
    {
        RedisTableDescription tpchTemplate;
        try (InputStream data = RedisTestUtils.class.getResourceAsStream(format("/tpch/%s/%s.json", dataFormat, schemaTableName.getTableName()))) {
            tpchTemplate = tableDescriptionJsonCodec.fromJson(ByteStreams.toByteArray(data));
        }

        RedisTableDescription tableDescription = new RedisTableDescription(
                schemaTableName.getTableName(),
                schemaTableName.getSchemaName(),
                tpchTemplate.getKey(),
                tpchTemplate.getValue());

        return new AbstractMap.SimpleImmutableEntry<>(schemaTableName, tableDescription);
    }

    public static Map.Entry<SchemaTableName, RedisTableDescription> createTableDescription(RedisTableDescription tableDescription)
    {
        SchemaTableName schemaTableName = new SchemaTableName(
                tableDescription.getSchemaName(),
                tableDescription.getTableName());

        return new AbstractMap.SimpleImmutableEntry<>(schemaTableName, tableDescription);
    }

    public static RedisTableDescription loadSimpleTableDescription(QueryRunner queryRunner, String valueDataFormat)
            throws Exception
    {
        JsonCodec<RedisTableDescription> tableDescriptionJsonCodec = new CodecSupplier<>(RedisTableDescription.class, queryRunner.getTypeManager()).get();
        try (InputStream data = RedisTestUtils.class.getResourceAsStream(format("/simple/%s_value_table.json", valueDataFormat))) {
            return tableDescriptionJsonCodec.fromJson(ByteStreams.toByteArray(data));
        }
    }
}
