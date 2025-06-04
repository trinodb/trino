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
import io.trino.metastore.HiveType;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

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
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)));
        JsonCodec<HiveColumnHandle> codec = new JsonCodecFactory(objectMapperProvider).jsonCodec(HiveColumnHandle.class);

        String json = codec.toJson(expected);
        HiveColumnHandle actual = codec.fromJson(json);

        assertThat(actual.getBaseColumnName()).isEqualTo(expected.getBaseColumnName());
        assertThat(actual.getBaseHiveColumnIndex()).isEqualTo(expected.getBaseHiveColumnIndex());
        assertThat(actual.getBaseType()).isEqualTo(expected.getBaseType());
        assertThat(actual.getBaseHiveType()).isEqualTo(expected.getBaseHiveType());

        assertThat(actual.getName()).isEqualTo(expected.getName());
        assertThat(actual.getType()).isEqualTo(expected.getType());
        assertThat(actual.getHiveType()).isEqualTo(expected.getHiveType());

        assertThat(actual.getHiveColumnProjectionInfo()).isEqualTo(expected.getHiveColumnProjectionInfo());
        assertThat(actual.isPartitionKey()).isEqualTo(expected.isPartitionKey());
    }
}
