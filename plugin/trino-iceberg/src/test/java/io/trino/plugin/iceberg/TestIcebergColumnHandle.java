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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.ARRAY;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.PRIMITIVE;
import static io.trino.plugin.iceberg.ColumnIdentity.TypeCategory.STRUCT;
import static io.trino.plugin.iceberg.ColumnIdentity.primitiveColumnIdentity;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.testng.Assert.assertEquals;

public class TestIcebergColumnHandle
{
    @Test
    public void testRoundTrip()
    {
        testRoundTrip(new IcebergColumnHandle(primitiveColumnIdentity(12, "blah"), BIGINT, ImmutableList.of(), BIGINT, Optional.of("this is a comment")));

        // Nested column
        ColumnIdentity foo1 = new ColumnIdentity(1, "foo1", PRIMITIVE, ImmutableList.of());
        ColumnIdentity foo2 = new ColumnIdentity(2, "foo2", PRIMITIVE, ImmutableList.of());
        ColumnIdentity foo3 = new ColumnIdentity(3, "foo3", ARRAY, ImmutableList.of(foo1));
        Type nestedColumnType = RowType.from(ImmutableList.of(
                RowType.field("foo2", BIGINT),
                RowType.field("foo3", new ArrayType(BIGINT))));
        IcebergColumnHandle nestedColumn = new IcebergColumnHandle(
                new ColumnIdentity(
                        5,
                        "foo5",
                        STRUCT,
                        ImmutableList.of(foo2, foo3)),
                nestedColumnType,
                ImmutableList.of(),
                nestedColumnType,
                Optional.empty());
        testRoundTrip(nestedColumn);

        IcebergColumnHandle partialColumn = new IcebergColumnHandle(
                new ColumnIdentity(
                        5,
                        "foo5",
                        STRUCT,
                        ImmutableList.of(foo2, foo3)),
                nestedColumnType,
                ImmutableList.of(2),
                BIGINT,
                Optional.empty());
        testRoundTrip(partialColumn);
    }

    private void testRoundTrip(IcebergColumnHandle expected)
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)));
        JsonCodec<IcebergColumnHandle> codec = new JsonCodecFactory(objectMapperProvider).jsonCodec(IcebergColumnHandle.class);

        String json = codec.toJson(expected);
        IcebergColumnHandle actual = codec.fromJson(json);

        assertEquals(actual, expected);
        assertEquals(actual.getBaseColumnIdentity(), expected.getBaseColumnIdentity());
        assertEquals(actual.getBaseType(), expected.getBaseType());
        assertEquals(actual.getQualifiedName(), expected.getQualifiedName());
        assertEquals(actual.getName(), expected.getName());
        assertEquals(actual.getColumnIdentity(), expected.getColumnIdentity());
        assertEquals(actual.getId(), actual.getId());
        assertEquals(actual.getType(), expected.getType());
        assertEquals(actual.getComment(), expected.getComment());
    }
}
