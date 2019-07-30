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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.airlift.json.JsonModule;
import io.prestosql.connector.informationschema.InformationSchemaTableHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestInformationSchemaTableHandle
{
    private ObjectMapper objectMapper;

    @BeforeMethod
    public void startUp()
    {
        Injector injector = Guice.createInjector(new JsonModule(), new HandleJsonModule());

        objectMapper = injector.getInstance(ObjectMapper.class);
    }

    @Test
    public void testBasic()
            throws Exception
    {
        InformationSchemaTableHandle tableHandleBasic = new InformationSchemaTableHandle(
                "information_schema_catalog",
                "information_schema_schema",
                "information_schema_table",
                Optional.of(ImmutableSet.of("test_schema1", "test_schema2")),
                Optional.of(ImmutableSet.of("test_table1", "test_table2")));

        InformationSchemaTableHandle tableHandleSchemaPrefixesEmpty = new InformationSchemaTableHandle(
                "information_schema_catalog",
                "information_schema_schema",
                "information_schema_table",
                Optional.empty(),
                Optional.of(ImmutableSet.of("test_table1", "test_table2")));

        InformationSchemaTableHandle tableHandleTableNameDomainEmpty = new InformationSchemaTableHandle(
                "information_schema_catalog",
                "information_schema_schema",
                "information_schema_table",
                Optional.of(ImmutableSet.of("test_schema1", "test_schema2")),
                Optional.empty());

        InformationSchemaTableHandle tableHandleBothEmpty = new InformationSchemaTableHandle(
                "information_schema_catalog",
                "information_schema_schema",
                "information_schema_table",
                Optional.empty(),
                Optional.empty());

        testSerDe(tableHandleBasic);
        testSerDe(tableHandleSchemaPrefixesEmpty);
        testSerDe(tableHandleTableNameDomainEmpty);
        testSerDe(tableHandleBothEmpty);
    }

    private void testSerDe(InformationSchemaTableHandle tableHandle)
            throws Exception
    {
        assertTrue(objectMapper.canSerialize(InformationSchemaTableHandle.class));
        String json = objectMapper.writeValueAsString(tableHandle);

        ConnectorTableHandle handleDeserialized = objectMapper.readValue(json, ConnectorTableHandle.class);
        assertEquals(tableHandle.getClass(), InformationSchemaTableHandle.class);
        InformationSchemaTableHandle tableHandleDeserialized = (InformationSchemaTableHandle) handleDeserialized;

        assertEquals(tableHandle, tableHandleDeserialized);
    }
}
