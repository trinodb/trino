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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static org.testng.Assert.assertEquals;

public class TestHiveColumnHandle
{
    @Test
    public void testHiddenColumn()
    {
        HiveColumnHandle hiddenColumn = HiveColumnHandle.pathColumnHandle();
        testRoundTrip(hiddenColumn);
    }

    @Test
    public void testRegularColumn()
    {
        HiveColumnHandle expectedPartitionColumn = new HiveColumnHandle("name", HiveType.HIVE_FLOAT, DOUBLE, 88, PARTITION_KEY, Optional.empty());
        testRoundTrip(expectedPartitionColumn);
    }

    @Test
    public void testPartitionKeyColumn()
    {
        HiveColumnHandle expectedRegularColumn = new HiveColumnHandle("name", HiveType.HIVE_FLOAT, DOUBLE, 88, REGULAR, Optional.empty());
        testRoundTrip(expectedRegularColumn);
    }

    private void testRoundTrip(HiveColumnHandle expected)
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new HiveModule.TypeDeserializer(new TestingTypeManager())));
        JsonCodec<HiveColumnHandle> codec = new JsonCodecFactory(objectMapperProvider).jsonCodec(HiveColumnHandle.class);

        String json = codec.toJson(expected);
        HiveColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getHiveType(), expected.getHiveType());
        assertEquals(actual.getHiveColumnIndex(), expected.getHiveColumnIndex());
        assertEquals(actual.isPartitionKey(), expected.isPartitionKey());
    }
}
