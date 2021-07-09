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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.type.InternalTypeManager;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Arrays.asList;
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
        HiveColumnHandle expectedPartitionColumn = createBaseColumn("name", 88, HiveType.HIVE_FLOAT, DOUBLE, PARTITION_KEY, Optional.empty());
        testRoundTrip(expectedPartitionColumn);
    }

    @Test
    public void testPartitionKeyColumn()
    {
        HiveColumnHandle expectedRegularColumn = createBaseColumn("name", 88, HiveType.HIVE_FLOAT, DOUBLE, REGULAR, Optional.empty());
        testRoundTrip(expectedRegularColumn);
    }

    @Test
    public void testProjectedColumn()
    {
        Type baseType = RowType.from(asList(field("a", VARCHAR), field("b", BIGINT)));
        HiveType baseHiveType = toHiveType(baseType);

        HiveColumnProjectionInfo columnProjectionInfo = new HiveColumnProjectionInfo(
                ImmutableList.of(1),
                ImmutableList.of("b"),
                HiveType.HIVE_LONG,
                BIGINT);

        HiveColumnHandle projectedColumn = new HiveColumnHandle(
                "struct_col",
                88,
                baseHiveType,
                baseType,
                Optional.of(columnProjectionInfo),
                REGULAR,
                Optional.empty());

        testRoundTrip(projectedColumn);
    }

    private void testRoundTrip(HiveColumnHandle expected)
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        TypeManager typeManager = new InternalTypeManager(createTestMetadataManager(), new TypeOperators());
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(typeManager)));
        JsonCodec<HiveColumnHandle> codec = new JsonCodecFactory(objectMapperProvider).jsonCodec(HiveColumnHandle.class);

        String json = codec.toJson(expected);
        HiveColumnHandle actual = codec.fromJson(json);

        assertEquals(actual.getBaseColumnName(), expected.getBaseColumnName());
        assertEquals(actual.getBaseHiveColumnIndex(), expected.getBaseHiveColumnIndex());
        assertEquals(actual.getBaseType(), expected.getBaseType());
        assertEquals(actual.getBaseHiveType(), expected.getBaseHiveType());

        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getType(), expected.getType());
        assertEquals(actual.getHiveType(), expected.getHiveType());

        assertEquals(actual.getHiveColumnProjectionInfo(), expected.getHiveColumnProjectionInfo());
        assertEquals(actual.isPartitionKey(), expected.isPartitionKey());
    }
}
