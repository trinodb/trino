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
package io.prestosql.plugin.prometheus;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.type.InternalTypeManager;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.plugin.prometheus.PrometheusClient.TIMESTAMP_COLUMN_TYPE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TypeSignature.mapType;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Locale.ENGLISH;

public final class MetadataUtil
{
    private MetadataUtil() {}

    public static final JsonCodec<PrometheusTable> TABLE_CODEC;
    public static final JsonCodec<PrometheusColumnHandle> COLUMN_CODEC;
    public static final JsonCodec<Map<String, Object>> METRIC_CODEC;

    private static final Metadata METADATA = createTestMetadataManager();
    private static final TypeManager TYPE_MANAGER = new InternalTypeManager(METADATA);
    static final MapType varcharMapType = (MapType) TYPE_MANAGER.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));

    static {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TestingTypeDeserializer()));
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        TABLE_CODEC = codecFactory.jsonCodec(PrometheusTable.class);
        COLUMN_CODEC = codecFactory.jsonCodec(PrometheusColumnHandle.class);
        METRIC_CODEC = codecFactory.mapJsonCodec(String.class, Object.class);
    }

    public static final class TestingTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final Map<String, Type> types = ImmutableMap.<String, Type>builder()
                .put(varcharMapType.getTypeSignature().toString(), varcharMapType)
                .put(StandardTypes.BIGINT, BIGINT)
                .put(StandardTypes.TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_COLUMN_TYPE)
                .put("timestamp(3) with time zone", TIMESTAMP_COLUMN_TYPE)
                .put(StandardTypes.DOUBLE, DOUBLE)
                .put(StandardTypes.VARCHAR, createUnboundedVarcharType())
                .build();

        public TestingTypeDeserializer()
        {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = types.get(value.toLowerCase(ENGLISH));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
