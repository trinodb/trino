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
package io.trino.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.airlift.json.JsonModule;
import io.trino.connector.informationschema.InformationSchemaTableHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.trino.connector.informationschema.InformationSchemaTable.COLUMNS;
import static io.trino.connector.informationschema.InformationSchemaTable.INFORMATION_SCHEMA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestInformationSchemaTableHandle
{
    private static final Map<String, Object> SCHEMA_AS_MAP = new ImmutableMap.Builder<String, Object>()
            .put("@type", "$info_schema")
            .put("catalogName", "information_schema_catalog")
            .put("table", "COLUMNS")
            .put("prefixes", ImmutableList.of(ImmutableMap.of("catalogName", "abc", "schemaName", INFORMATION_SCHEMA)))
            .put("roles", ImmutableList.of("role"))
            .put("grantees", ImmutableList.of("grantee"))
            .buildOrThrow();

    private ObjectMapper objectMapper;

    @BeforeMethod
    public void startUp()
    {
        Injector injector = Guice.createInjector(new JsonModule(), new HandleJsonModule());

        objectMapper = injector.getInstance(ObjectMapper.class);
    }

    @Test
    public void testInformationSchemaSerialize()
            throws Exception
    {
        InformationSchemaTableHandle informationSchemaTableHandle = new InformationSchemaTableHandle(
                "information_schema_catalog",
                COLUMNS,
                ImmutableSet.of(new QualifiedTablePrefix("abc", INFORMATION_SCHEMA)),
                Optional.of(ImmutableSet.of("role")),
                Optional.of(ImmutableSet.of("grantee")),
                OptionalLong.empty());

        assertTrue(objectMapper.canSerialize(InformationSchemaTableHandle.class));
        String json = objectMapper.writeValueAsString(informationSchemaTableHandle);
        testJsonEquals(json, SCHEMA_AS_MAP);
    }

    @Test
    public void testInformationSchemaDeserialize()
            throws Exception
    {
        String json = objectMapper.writeValueAsString(SCHEMA_AS_MAP);

        ConnectorTableHandle tableHandle = objectMapper.readValue(json, ConnectorTableHandle.class);
        assertEquals(tableHandle.getClass(), InformationSchemaTableHandle.class);
        InformationSchemaTableHandle informationSchemaHandle = (InformationSchemaTableHandle) tableHandle;

        assertEquals(informationSchemaHandle.getCatalogName(), "information_schema_catalog");
        assertEquals(informationSchemaHandle.getTable(), COLUMNS);
    }

    private void testJsonEquals(String json, Map<String, Object> expectedMap)
            throws Exception
    {
        Map<String, Object> jsonMap = objectMapper.readValue(json, new TypeReference<>() {});
        assertEqualsIgnoreOrder(jsonMap.entrySet(), expectedMap.entrySet());
    }
}
