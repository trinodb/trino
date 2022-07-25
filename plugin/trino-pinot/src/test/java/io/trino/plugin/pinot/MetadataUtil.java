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
package io.trino.plugin.pinot;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.utils.DataSchema;

import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class MetadataUtil
{
    public static final JsonCodec<PinotColumnHandle> COLUMN_CODEC;
    public static final JsonCodec<PinotClient.GetTables> TABLES_JSON_CODEC;
    public static final JsonCodec<PinotClient.BrokersForTable> BROKERS_FOR_TABLE_JSON_CODEC;
    public static final JsonCodec<PinotClient.TimeBoundary> TIME_BOUNDARY_JSON_CODEC;
    public static final JsonCodec<BrokerResponseNative> BROKER_RESPONSE_NATIVE_JSON_CODEC;
    public static final String TEST_TABLE = "airlineStats";

    private MetadataUtil()
    {
    }

    public static final class TestingTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final Map<String, Type> types = ImmutableMap.of(
                StandardTypes.BOOLEAN, BOOLEAN,
                StandardTypes.BIGINT, BIGINT,
                StandardTypes.INTEGER, INTEGER,
                StandardTypes.DOUBLE, DOUBLE,
                StandardTypes.VARCHAR, VARCHAR);

        public TestingTypeDeserializer()
        {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = types.get(value.toLowerCase(ENGLISH));
            return requireNonNull(type, "Unknown type " + value);
        }
    }

    static {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.<Class<?>, JsonDeserializer<?>>builder()
                .put(Type.class, new TestingTypeDeserializer())
                .put(DataSchema.class, new PinotModule.DataSchemaDeserializer())
                .buildOrThrow());
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        COLUMN_CODEC = codecFactory.jsonCodec(PinotColumnHandle.class);
        TABLES_JSON_CODEC = codecFactory.jsonCodec(PinotClient.GetTables.class);
        BROKERS_FOR_TABLE_JSON_CODEC = codecFactory.jsonCodec(PinotClient.BrokersForTable.class);
        TIME_BOUNDARY_JSON_CODEC = codecFactory.jsonCodec(PinotClient.TimeBoundary.class);
        BROKER_RESPONSE_NATIVE_JSON_CODEC = codecFactory.jsonCodec(BrokerResponseNative.class);
    }
}
