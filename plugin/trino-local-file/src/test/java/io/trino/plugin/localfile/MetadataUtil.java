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
package io.trino.plugin.localfile;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;

import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;

final class MetadataUtil
{
    private MetadataUtil()
    {
    }

    public static final JsonCodec<LocalFileColumnHandle> COLUMN_CODEC;
    public static final JsonCodec<LocalFileTableHandle> TABLE_CODEC;

    static {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TestingTypeDeserializer()));
        JsonCodecFactory codecFactory = new JsonCodecFactory(objectMapperProvider);
        COLUMN_CODEC = codecFactory.jsonCodec(LocalFileColumnHandle.class);
        TABLE_CODEC = codecFactory.jsonCodec(LocalFileTableHandle.class);
    }

    public static final class TestingTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final Map<TypeId, Type> types = ImmutableMap.<TypeId, Type>builder()
                .put(BOOLEAN.getTypeId(), BOOLEAN)
                .put(BIGINT.getTypeId(), BIGINT)
                .put(DOUBLE.getTypeId(), DOUBLE)
                .put(createTimestampWithTimeZoneType(3).getTypeId(), createTimestampWithTimeZoneType(3))
                .put(DATE.getTypeId(), DATE)
                .put(VARCHAR.getTypeId(), createUnboundedVarcharType())
                .buildOrThrow();

        public TestingTypeDeserializer()
        {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = types.get(TypeId.of(value));
            if (type == null) {
                throw new IllegalArgumentException("Unknown type " + value);
            }
            return type;
        }
    }
}
