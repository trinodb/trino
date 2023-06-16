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
package io.trino.plugin.openpolicyagent;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.trino.plugin.openpolicyagent.schema.TrinoUser;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.trino.plugin.openpolicyagent.RequestTestUtilities.assertJsonRequestsEqual;
import static io.trino.plugin.openpolicyagent.RequestTestUtilities.assertStringRequestsEqual;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpaBatchAccessControlFilteringUnitTest
{
    private static URI opaServerUri = URI.create("http://my-uri/");
    private static URI opaBatchServerUri = URI.create("http://my-uri/batchAllow");
    private HttpClientUtils.InstrumentedHttpClient mockClient;
    private OpaAccessControl authorizer;
    private JsonMapper jsonMapper = new JsonMapper();
    private Identity requestingIdentity;
    private SystemSecurityContext requestingSecurityContext;

    @BeforeEach
    public void setupAuthorizer()
    {
        this.jsonMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        this.jsonMapper.registerModule(new Jdk8Module());
        this.mockClient = new HttpClientUtils.InstrumentedHttpClient(opaBatchServerUri, "POST", JSON_UTF_8.toString(), (request) -> null);
        this.authorizer = (OpaAccessControl) new OpaAccessControlFactory()
                .create(
                        Map.of(
                                "opa.policy.uri", opaServerUri.toString(),
                                "opa.policy.batched-uri", opaBatchServerUri.toString()),
                        Optional.of(mockClient));
        this.requestingIdentity = Identity.ofUser("source-user");
        this.requestingSecurityContext = new SystemSecurityContext(requestingIdentity, Optional.empty());
    }

    @AfterEach
    public void ensureRequestContextCorrect()
            throws IOException
    {
        for (String request : mockClient.getRequests()) {
            JsonNode parsedRequest = jsonMapper.readTree(request);
            assertEquals(parsedRequest.at("/input/context/identity/user").asText(), requestingIdentity.getUser());
        }
    }

    private ObjectNode encodeObjectWithKey(Object inp, String key)
    {
        return jsonMapper.createObjectNode().set(key, jsonMapper.valueToTree(inp));
    }

    private static Stream<Arguments> subsetProvider()
    {
        return Stream.of(
                Arguments.of(Named.of("All-3-resources", new HttpClientUtils.MockResponse("{\"result\": [0, 1, 2]}", 200)), List.of(0, 1, 2)),
                Arguments.of(Named.of("First-and-last-resources", new HttpClientUtils.MockResponse("{\"result\": [0, 2]}", 200)), List.of(0, 2)),
                Arguments.of(Named.of("Only-one-resource", new HttpClientUtils.MockResponse("{\"result\": [2]}", 200)), List.of(2)),
                Arguments.of(Named.of("No-resources", new HttpClientUtils.MockResponse("{\"result\": []}", 200)), List.of()));
    }

    private <T> List<T> getSubset(List<T> allItems, List<Integer> subsetPositions)
    {
        List<T> result = new ArrayList<>();
        for (int i : subsetPositions) {
            if (i < 0 || i >= allItems.size()) {
                throw new IllegalArgumentException("Invalid subset of items provided");
            }
            result.add(allItems.get(i));
        }
        return result;
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaBatchAccessControlFilteringUnitTest#subsetProvider")
    public void testFilterViewQueryOwnedBy(
            HttpClientUtils.MockResponse response,
            List<Integer> expectedItems)
    {
        this.mockClient.setHandler((request) -> response);

        Identity identityOne = Identity.ofUser("user-one");
        Identity identityTwo = Identity.ofUser("user-two");
        Identity identityThree = Identity.ofUser("user-three");
        List<Identity> requestedIdentities = List.of(identityOne, identityTwo, identityThree);

        Collection<Identity> result = authorizer.filterViewQueryOwnedBy(
                requestingSecurityContext,
                requestedIdentities);
        assertEquals(Set.copyOf(result), Set.copyOf(getSubset(requestedIdentities, expectedItems)));

        ArrayNode allExpectedUsers = jsonMapper.createArrayNode().addAll(
                requestedIdentities.stream()
                        .map(TrinoUser::new)
                        .map((i) -> encodeObjectWithKey(i, "user"))
                        .toList());
        ObjectNode expectedRequest = jsonMapper.createObjectNode()
                .put("operation", "FilterViewQueryOwnedBy")
                .set("filterResources", allExpectedUsers);
        assertJsonRequestsEqual(Set.of(expectedRequest), this.mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaBatchAccessControlFilteringUnitTest#subsetProvider")
    public void testFilterCatalogs(
            HttpClientUtils.MockResponse response,
            List<Integer> expectedItems)
    {
        this.mockClient.setHandler((request) -> response);

        List<String> requestedCatalogs = List.of("catalog-one", "catalog-two", "catalog-three");

        Set<String> result = authorizer.filterCatalogs(
                requestingSecurityContext,
                new LinkedHashSet<>(requestedCatalogs));
        assertEquals(Set.copyOf(result), Set.copyOf(getSubset(requestedCatalogs, expectedItems)));

        String expectedRequest = """
                {
                    "operation": "FilterCatalogs",
                    "filterResources": [
                        {
                            "catalog": {
                                "name": "catalog-one"
                            }
                        },
                        {
                            "catalog": {
                                "name": "catalog-two"
                            }
                        },
                        {
                            "catalog": {
                                "name": "catalog-three"
                            }
                        }
                    ]
                }""";
        assertStringRequestsEqual(List.of(expectedRequest), this.mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaBatchAccessControlFilteringUnitTest#subsetProvider")
    public void testFilterSchemas(
            HttpClientUtils.MockResponse response,
            List<Integer> expectedItems)
    {
        this.mockClient.setHandler((request) -> response);
        List<String> requestedSchemas = List.of("schema-one", "schema-two", "schema-three");

        Set<String> result = authorizer.filterSchemas(
                requestingSecurityContext,
                "my-catalog",
                new LinkedHashSet<>(requestedSchemas));
        assertEquals(Set.copyOf(result), Set.copyOf(getSubset(requestedSchemas, expectedItems)));

        String expectedRequest = """
                {
                    "operation": "FilterSchemas",
                    "filterResources": [
                        {
                            "schema": {
                                "schemaName": "schema-one",
                                "catalogName": "my-catalog"
                            }
                        },
                        {
                            "schema": {
                                "schemaName": "schema-two",
                                "catalogName": "my-catalog"
                            }
                        },
                        {
                            "schema": {
                                "schemaName": "schema-three",
                                "catalogName": "my-catalog"
                            }
                        }
                    ]
                }""";
        assertStringRequestsEqual(List.of(expectedRequest), this.mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaBatchAccessControlFilteringUnitTest#subsetProvider")
    public void testFilterTables(
            HttpClientUtils.MockResponse response,
            List<Integer> expectedItems)
    {
        this.mockClient.setHandler((request) -> response);
        List<SchemaTableName> tables = List.of(
                new SchemaTableName("schema-one", "table-one"),
                new SchemaTableName("schema-one", "table-two"),
                new SchemaTableName("schema-two", "table-one"));

        Set<SchemaTableName> result = authorizer.filterTables(
                requestingSecurityContext,
                "my-catalog",
                new LinkedHashSet<>(tables));
        assertEquals(Set.copyOf(result), Set.copyOf(getSubset(tables, expectedItems)));

        String expectedRequest = """
                {
                    "operation": "FilterTables",
                    "filterResources": [
                        {
                            "table": {
                                "tableName": "table-one",
                                "schemaName": "schema-one",
                                "catalogName": "my-catalog"
                            }
                        },
                        {
                            "table": {
                                "tableName": "table-two",
                                "schemaName": "schema-one",
                                "catalogName": "my-catalog"
                            }
                        },
                        {
                            "table": {
                                "tableName": "table-one",
                                "schemaName": "schema-two",
                                "catalogName": "my-catalog"
                            }
                        }
                    ]
                }""";
        assertStringRequestsEqual(List.of(expectedRequest), this.mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaBatchAccessControlFilteringUnitTest#subsetProvider")
    public void testFilterColumns(
            HttpClientUtils.MockResponse response,
            List<Integer> expectedItems)
    {
        this.mockClient.setHandler((request) -> response);
        CatalogSchemaTableName table = new CatalogSchemaTableName("my-catalog", "my-schema", "my-table");
        List<String> requestedColumns = List.of("column-one", "column-two", "column-three");

        Set<String> result = authorizer.filterColumns(
                requestingSecurityContext,
                table,
                new LinkedHashSet<>(requestedColumns));
        assertEquals(Set.copyOf(result), Set.copyOf(getSubset(requestedColumns, expectedItems)));

        String expectedRequest = """
                {
                    "operation": "FilterColumns",
                    "filterResources": [
                        {
                            "table": {
                                "tableName": "my-table",
                                "schemaName": "my-schema",
                                "catalogName": "my-catalog",
                                "columns": ["column-one", "column-two", "column-three"]
                            }
                        }
                    ]
                }""";
        assertStringRequestsEqual(List.of(expectedRequest), this.mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.FilteringTestHelpers#emptyInputTestCases")
    public void testEmptyRequests(
            BiFunction<OpaAccessControl, SystemSecurityContext, Collection> callable)
    {
        Collection result = callable.apply(authorizer, requestingSecurityContext);
        assertEquals(result.size(), 0);
        assertEquals(mockClient.getRequests().size(), 0);
    }

    @ParameterizedTest(name = "{index}: {0} - {1}")
    @MethodSource("io.trino.plugin.openpolicyagent.FilteringTestHelpers#prepopulatedErrorCases")
    public void testIllegalResponseThrows(
            BiFunction<OpaAccessControl, SystemSecurityContext, Collection> callable,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler((request) -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> callable.apply(authorizer, requestingSecurityContext));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
        assertTrue(mockClient.getRequests().size() > 0);
    }
}
