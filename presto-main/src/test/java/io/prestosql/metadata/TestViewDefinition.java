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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.metadata.ViewDefinition.ViewColumn;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.Type;
import io.prestosql.type.TypeDeserializer;
import io.prestosql.type.TypeRegistry;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestViewDefinition
{
    private static final JsonCodec<ViewDefinition> CODEC = createTestingViewCodec();
    private static final String BASE_JSON = "" +
            "\"originalSql\": \"SELECT 42 x\", " +
            "\"columns\": [{\"name\": \"x\", \"type\": \"integer\"}]";

    private static JsonCodec<ViewDefinition> createTestingViewCodec()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(new TypeRegistry())));
        return new JsonCodecFactory(provider).jsonCodec(ViewDefinition.class);
    }

    @Test
    public void testLegacyViewWithoutOwner()
    {
        // very old view before owner was added
        ViewDefinition view = CODEC.fromJson("{" + BASE_JSON + "}");
        assertBaseView(view);
        assertFalse(view.getOwner().isPresent());
    }

    @Test
    public void testViewWithOwner()
    {
        // old view before invoker security was added
        ViewDefinition view = CODEC.fromJson("{" + BASE_JSON + ", \"owner\": \"abc\"}");
        assertBaseView(view);
        assertEquals(view.getOwner(), Optional.of("abc"));
        assertFalse(view.isRunAsInvoker());
    }

    @Test
    public void testViewSecurityDefiner()
    {
        ViewDefinition view = CODEC.fromJson("{" + BASE_JSON + ", \"owner\": \"abc\", \"runAsInvoker\": false}");
        assertBaseView(view);
        assertEquals(view.getOwner(), Optional.of("abc"));
        assertFalse(view.isRunAsInvoker());
    }

    @Test
    public void testViewSecurityInvoker()
    {
        ViewDefinition view = CODEC.fromJson("{" + BASE_JSON + ", \"runAsInvoker\": true}");
        assertBaseView(view);
        assertFalse(view.getOwner().isPresent());
        assertTrue(view.isRunAsInvoker());
    }

    private static void assertBaseView(ViewDefinition view)
    {
        assertEquals(view.getOriginalSql(), "SELECT 42 x");
        assertEquals(view.getColumns().size(), 1);
        ViewColumn column = getOnlyElement(view.getColumns());
        assertEquals(column.getName(), "x");
        assertEquals(column.getType(), IntegerType.INTEGER);
    }
}
