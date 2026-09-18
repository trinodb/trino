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
package io.trino.plugin.prometheus;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;

import java.util.Map;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;

public final class MetadataUtil
{
    private MetadataUtil() {}

    public static final JsonCodec<PrometheusColumnHandle> COLUMN_CODEC;
    public static final JsonCodec<Map<String, Object>> METRIC_CODEC;

    static final MapType varcharMapType = new MapType(VARCHAR, VARCHAR, TESTING_TYPE_MANAGER.getTypeOperators());

    static {
        JsonMapper objectMapper = new JsonMapperProvider()
                .withJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)))
                .get();
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapper);
        COLUMN_CODEC = codecFactory.jsonCodec(PrometheusColumnHandle.class);
        METRIC_CODEC = codecFactory.mapJsonCodec(String.class, Object.class);
    }
}
