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
package io.trino.spi.connector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.TestingTypeDeserializer;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.util.Comparator.comparing;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConnectorViewDefinition
{
    private static final JsonCodec<ConnectorViewDefinition> CODEC = createTestingViewCodec();
    private static final String BASE_JSON = "" +
            "\"originalSql\": \"SELECT 42 x\", " +
            "\"columns\": [{\"name\": \"x\", \"type\": \"bigint\"}]";

    private static JsonCodec<ConnectorViewDefinition> createTestingViewCodec()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonDeserializers(ImmutableMap.of(Type.class, new TestingTypeDeserializer(new TestingTypeManager())));
        return new JsonCodecFactory(provider).jsonCodec(ConnectorViewDefinition.class);
    }

    @Test
    public void testLegacyViewWithoutOwner()
    {
        // very old view before owner was added
        ConnectorViewDefinition view = CODEC.fromJson("{" + BASE_JSON + "}");
        assertBaseView(view);
        assertThat(view.getOwner()).isNotPresent();
    }

    @Test
    public void testViewWithOwner()
    {
        // old view before invoker security was added
        ConnectorViewDefinition view = CODEC.fromJson("{" + BASE_JSON + ", \"owner\": \"abc\"}");
        assertBaseView(view);
        assertThat(view.getOwner()).isEqualTo(Optional.of("abc"));
        assertThat(view.isRunAsInvoker()).isFalse();
    }

    @Test
    public void testViewComment()
    {
        ConnectorViewDefinition view = CODEC.fromJson("{" + BASE_JSON + ", \"comment\": \"hello\"}");
        assertBaseView(view);
        assertThat(view.getComment()).isEqualTo(Optional.of("hello"));
    }

    @Test
    public void testViewSecurityDefiner()
    {
        ConnectorViewDefinition view = CODEC.fromJson("{" + BASE_JSON + ", \"owner\": \"abc\", \"runAsInvoker\": false}");
        assertBaseView(view);
        assertThat(view.getOwner()).isEqualTo(Optional.of("abc"));
        assertThat(view.isRunAsInvoker()).isFalse();
    }

    @Test
    public void testViewSecurityInvoker()
    {
        ConnectorViewDefinition view = CODEC.fromJson("{" + BASE_JSON + ", \"runAsInvoker\": true}");
        assertBaseView(view);
        assertThat(view.getOwner()).isNotPresent();
        assertThat(view.isRunAsInvoker()).isTrue();
    }

    @Test
    public void testRoundTrip()
    {
        assertRoundTrip(new ConnectorViewDefinition(
                "test view SQL",
                Optional.of("test_catalog"),
                Optional.of("test_schema"),
                ImmutableList.of(
                        new ViewColumn("abc", BIGINT.getTypeId(), Optional.of("abc description")),
                        new ViewColumn("xyz", new ArrayType(createVarcharType(32)).getTypeId(), Optional.empty())),
                Optional.of("comment"),
                Optional.of("test_owner"),
                false,
                ImmutableList.of()));
    }

    private static void assertBaseView(ConnectorViewDefinition view)
    {
        assertThat(view.getOriginalSql()).isEqualTo("SELECT 42 x");
        assertThat(view.getColumns().size()).isEqualTo(1);
        ViewColumn column = getOnlyElement(view.getColumns());
        assertThat(column.getName()).isEqualTo("x");
        assertThat(column.getType()).isEqualTo(BIGINT.getTypeId());
        assertRoundTrip(view);
    }

    private static void assertRoundTrip(ConnectorViewDefinition expected)
    {
        ConnectorViewDefinition actual = CODEC.fromJson(CODEC.toJson(expected));
        assertThat(actual.getOwner()).isEqualTo(expected.getOwner());
        assertThat(actual.isRunAsInvoker()).isEqualTo(expected.isRunAsInvoker());
        assertThat(actual.getCatalog()).isEqualTo(expected.getCatalog());
        assertThat(actual.getSchema()).isEqualTo(expected.getSchema());
        assertThat(actual.getOriginalSql()).isEqualTo(expected.getOriginalSql());
        assertThat(actual.getColumns())
                .usingElementComparator(columnComparator())
                .isEqualTo(expected.getColumns());
    }

    private static Comparator<ViewColumn> columnComparator()
    {
        return comparing(ViewColumn::getName)
                .thenComparing(column -> column.getType().toString());
    }
}
