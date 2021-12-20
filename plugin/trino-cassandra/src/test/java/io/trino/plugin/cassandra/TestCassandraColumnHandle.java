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
package io.trino.plugin.cassandra;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.spi.type.Type;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.testng.Assert.assertEquals;

public class TestCassandraColumnHandle
{
    private JsonCodec<CassandraColumnHandle> codec;

    @BeforeClass
    public void setup()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)));
        codec = new JsonCodecFactory(objectMapperProvider).jsonCodec(CassandraColumnHandle.class);
    }

    @Test
    public void testRoundTrip()
    {
        CassandraColumnHandle expected = new CassandraColumnHandle("name", 42, CassandraTypes.FLOAT, true, false, false, false);

        String json = codec.toJson(expected);
        CassandraColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getOrdinalPosition(), expected.getOrdinalPosition());
        assertEquals(actual.getCassandraType(), expected.getCassandraType());
        assertEquals(actual.isPartitionKey(), expected.isPartitionKey());
        assertEquals(actual.isClusteringKey(), expected.isClusteringKey());
    }

    @Test
    public void testRoundTrip2()
    {
        CassandraColumnHandle expected = new CassandraColumnHandle(
                "name2",
                1,
                CassandraTypes.MAP,
                false,
                true,
                false,
                false);

        String json = codec.toJson(expected);
        CassandraColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getOrdinalPosition(), expected.getOrdinalPosition());
        assertEquals(actual.getCassandraType(), expected.getCassandraType());
        assertEquals(actual.isPartitionKey(), expected.isPartitionKey());
        assertEquals(actual.isClusteringKey(), expected.isClusteringKey());
    }
}
