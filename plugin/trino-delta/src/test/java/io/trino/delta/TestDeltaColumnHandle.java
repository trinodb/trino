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
package io.trino.delta;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.type.TypeSignature;
import io.trino.type.TypeSignatureDeserializer;
import org.testng.annotations.Test;

import static io.trino.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static io.trino.delta.DeltaColumnHandle.ColumnType.REGULAR;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static org.testng.Assert.assertEquals;

/**
 * Test {@link DeltaColumnHandle} is created with correct parameters and round trip JSON SerDe works.
 */
public class TestDeltaColumnHandle
{
    @Test
    public void testPartitionColumn()
    {
        DeltaColumnHandle expectedPartitionColumn = new DeltaColumnHandle(
                "name",
                "partCol",
                DOUBLE.getTypeSignature(),
                PARTITION);
        testRoundTrip(expectedPartitionColumn);
    }

    @Test
    public void testRegularColumn()
    {
        DeltaColumnHandle expectedRegularColumn = new DeltaColumnHandle(
                "name",
                "regularCol",
                DOUBLE.getTypeSignature(),
                REGULAR);
        testRoundTrip(expectedRegularColumn);
    }

    private void testRoundTrip(DeltaColumnHandle expected)
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonDeserializers(ImmutableMap.of(
                TypeSignature.class, new TypeSignatureDeserializer()));
        JsonCodec<DeltaColumnHandle> codec = new JsonCodecFactory(provider)
                .jsonCodec(DeltaColumnHandle.class);
        String json = codec.toJson(expected);
        DeltaColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getConnectorId(), expected.getConnectorId());
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getColumnType(), expected.getColumnType());
        assertEquals(actual.getDataType(), expected.getDataType());
    }
}
