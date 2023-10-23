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
package io.trino.plugin.opa;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.trino.plugin.opa.schema.TrinoUser;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
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
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.trino.plugin.opa.RequestTestUtilities.assertJsonRequestsEqual;
import static io.trino.plugin.opa.RequestTestUtilities.assertStringRequestsEqual;
import static io.trino.plugin.opa.TestHelpers.systemSecurityContextFromIdentity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpaBatchAccessControlFilteringUnitTest
{
    private static final URI OPA_SERVER_URI = URI.create("http://my-uri/");
    private static final URI OPA_BATCH_SERVER_URI = URI.create("http://my-uri/batchAllow");
    private HttpClientUtils.InstrumentedHttpClient mockClient;
    private OpaAccessControl authorizer;
    private final JsonMapper jsonMapper = new JsonMapper();
    private Identity requestingIdentity;
    private SystemSecurityContext requestingSecurityContext;

    @BeforeEach
    public void setupAuthorizer()
    {
        this.jsonMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        this.jsonMapper.registerModule(new Jdk8Module());
        this.mockClient = new HttpClientUtils.InstrumentedHttpClient(OPA_BATCH_SERVER_URI, "POST", JSON_UTF_8.toString(), request -> null);
        this.authorizer = (OpaAccessControl) new OpaAccessControlFactory().create(
                ImmutableMap.<String, String>builder()
                        .put("opa.policy.uri", OPA_SERVER_URI.toString())
                        .put("opa.policy.batched-uri", OPA_BATCH_SERVER_URI.toString())
                        .buildOrThrow(),
                Optional.of(mockClient));
        this.requestingIdentity = Identity.ofUser("source-user");
        this.requestingSecurityContext = systemSecurityContextFromIdentity(requestingIdentity);
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

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.OpaBatchAccessControlFilteringUnitTest#subsetProvider")
    public void testFilterViewQueryOwnedBy(
            HttpClientUtils.MockResponse response,
            List<Integer> expectedItems)
    {
        this.mockClient.setHandler(request -> response);

        Identity identityOne = Identity.ofUser("user-one");
        Identity identityTwo = Identity.ofUser("user-two");
        Identity identityThree = Identity.ofUser("user-three");
        List<Identity> requestedIdentities = ImmutableList.of(identityOne, identityTwo, identityThree);

        Collection<Identity> result = authorizer.filterViewQueryOwnedBy(requestingIdentity, requestedIdentities);
        assertEquals(ImmutableSet.copyOf(result), ImmutableSet.copyOf(getSubset(requestedIdentities, expectedItems)));

        ArrayNode allExpectedUsers = jsonMapper.createArrayNode().addAll(
                requestedIdentities.stream()
                        .map(TrinoUser::new)
                        .map(user -> encodeObjectWithKey(user, "user"))
                        .collect(toImmutableList()));
        ObjectNode expectedRequest = jsonMapper.createObjectNode()
                .put("operation", "FilterViewQueryOwnedBy")
                .set("filterResources", allExpectedUsers);
        assertJsonRequestsEqual(ImmutableSet.of(expectedRequest), this.mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.OpaBatchAccessControlFilteringUnitTest#subsetProvider")
    public void testFilterCatalogs(
            HttpClientUtils.MockResponse response,
            List<Integer> expectedItems)
    {
        this.mockClient.setHandler(request -> response);

        List<String> requestedCatalogs = ImmutableList.of("catalog_one", "catalog_two", "catalog_three");

        Set<String> result = authorizer.filterCatalogs(
                requestingSecurityContext,
                new LinkedHashSet<>(requestedCatalogs));
        assertEquals(ImmutableSet.copyOf(result), ImmutableSet.copyOf(getSubset(requestedCatalogs, expectedItems)));

        String expectedRequest = """
                {
                    "operation": "FilterCatalogs",
                    "filterResources": [
                        {
                            "catalog": {
                                "name": "catalog_one"
                            }
                        },
                        {
                            "catalog": {
                                "name": "catalog_two"
                            }
                        },
                        {
                            "catalog": {
                                "name": "catalog_three"
                            }
                        }
                    ]
                }""";
        assertStringRequestsEqual(ImmutableList.of(expectedRequest), this.mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.OpaBatchAccessControlFilteringUnitTest#subsetProvider")
    public void testFilterSchemas(
            HttpClientUtils.MockResponse response,
            List<Integer> expectedItems)
    {
        this.mockClient.setHandler(request -> response);
        List<String> requestedSchemas = ImmutableList.of("schema_one", "schema_two", "schema_three");

        Set<String> result = authorizer.filterSchemas(
                requestingSecurityContext,
                "my_catalog",
                new LinkedHashSet<>(requestedSchemas));
        assertEquals(ImmutableSet.copyOf(result), ImmutableSet.copyOf(getSubset(requestedSchemas, expectedItems)));

        String expectedRequest = """
                {
                    "operation": "FilterSchemas",
                    "filterResources": [
                        {
                            "schema": {
                                "schemaName": "schema_one",
                                "catalogName": "my_catalog"
                            }
                        },
                        {
                            "schema": {
                                "schemaName": "schema_two",
                                "catalogName": "my_catalog"
                            }
                        },
                        {
                            "schema": {
                                "schemaName": "schema_three",
                                "catalogName": "my_catalog"
                            }
                        }
                    ]
                }""";
        assertStringRequestsEqual(ImmutableList.of(expectedRequest), this.mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.OpaBatchAccessControlFilteringUnitTest#subsetProvider")
    public void testFilterTables(
            HttpClientUtils.MockResponse response,
            List<Integer> expectedItems)
    {
        this.mockClient.setHandler(request -> response);
        List<SchemaTableName> tables = ImmutableList.<SchemaTableName>builder()
                .add(new SchemaTableName("schema_one", "table_one"))
                .add(new SchemaTableName("schema_one", "table_two"))
                .add(new SchemaTableName("schema_two", "table_one"))
                .build();

        Set<SchemaTableName> result = authorizer.filterTables(
                requestingSecurityContext,
                "my_catalog",
                new LinkedHashSet<>(tables));
        assertEquals(ImmutableSet.copyOf(result), ImmutableSet.copyOf(getSubset(tables, expectedItems)));

        String expectedRequest = """
                {
                    "operation": "FilterTables",
                    "filterResources": [
                        {
                            "table": {
                                "tableName": "table_one",
                                "schemaName": "schema_one",
                                "catalogName": "my_catalog"
                            }
                        },
                        {
                            "table": {
                                "tableName": "table_two",
                                "schemaName": "schema_one",
                                "catalogName": "my_catalog"
                            }
                        },
                        {
                            "table": {
                                "tableName": "table_one",
                                "schemaName": "schema_two",
                                "catalogName": "my_catalog"
                            }
                        }
                    ]
                }""";
        assertStringRequestsEqual(ImmutableList.of(expectedRequest), this.mockClient.getRequests(), "/input/action");
    }

    private static Function<String, HttpClientUtils.MockResponse> buildHandler(Function<String, String> dataBuilder)
    {
        return request -> new HttpClientUtils.MockResponse(dataBuilder.apply(request), 200);
    }

    @Test
    public void testFilterColumns()
    {
        SchemaTableName tableOne = SchemaTableName.schemaTableName("my_schema", "table_one");
        SchemaTableName tableTwo = SchemaTableName.schemaTableName("my_schema", "table_two");
        SchemaTableName tableThree = SchemaTableName.schemaTableName("my_schema", "table_three");
        Map<SchemaTableName, Set<String>> requestedColumns = ImmutableMap.<SchemaTableName, Set<String>>builder()
                .put(tableOne, ImmutableSet.of("table_one_column_one", "table_one_column_two"))
                .put(tableTwo, ImmutableSet.of("table_two_column_one", "table_two_column_two"))
                .put(tableThree, ImmutableSet.of("table_three_column_one", "table_three_column_two"))
                .buildOrThrow();

        // Allow both columns from one table, one column from another one and no columns from the last one
        this.mockClient.setHandler(
                buildHandler(
                        request -> {
                            if (request.contains("table_one")) {
                                return "{\"result\": [0, 1]}";
                            } else if (request.contains("table_two")) {
                                return "{\"result\": [1]}";
                            }
                            return "{\"result\": []}";
                        }));

        Map<SchemaTableName, Set<String>> result = authorizer.filterColumns(
                requestingSecurityContext,
                "my_catalog",
                requestedColumns);

        List<String> expectedRequests = Stream.of("table_one", "table_two", "table_three")
                .map(tableName -> """
                        {
                            "operation": "FilterColumns",
                            "filterResources": [
                                {
                                    "table": {
                                        "tableName": "%s",
                                        "schemaName": "my_schema",
                                        "catalogName": "my_catalog",
                                        "columns": ["%s_column_one", "%s_column_two"]
                                    }
                                }
                            ]
                        }
                        """.formatted(tableName, tableName, tableName))
                .collect(toImmutableList());
        assertStringRequestsEqual(expectedRequests, this.mockClient.getRequests(), "/input/action");
        assertTrue(Maps.difference(
                result,
                ImmutableMap.builder()
                        .put(tableOne, ImmutableSet.of("table_one_column_one", "table_one_column_two"))
                        .put(tableTwo, ImmutableSet.of("table_two_column_two"))
                        .put(tableThree, ImmutableSet.of())
                        .buildOrThrow()).areEqual());
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.OpaBatchAccessControlFilteringUnitTest#subsetProvider")
    public void testFilterFunctions(
            HttpClientUtils.MockResponse response,
            List<Integer> expectedItems)
    {
        this.mockClient.setHandler(request -> response);
        List<SchemaFunctionName> requestedFunctions = ImmutableList.<SchemaFunctionName>builder()
                .add(new SchemaFunctionName("my_schema", "function_one"))
                .add(new SchemaFunctionName("my_schema", "function_two"))
                .add(new SchemaFunctionName("my_schema", "function_three"))
                .build();

        Set<SchemaFunctionName> result = authorizer.filterFunctions(
                requestingSecurityContext,
                "my_catalog",
                new LinkedHashSet<>(requestedFunctions));
        assertEquals(ImmutableSet.copyOf(result), ImmutableSet.copyOf(getSubset(requestedFunctions, expectedItems)));

        String expectedRequest = """
                {
                    "operation": "FilterFunctions",
                    "filterResources": [
                        {
                            "function": {
                                "catalogName": "my_catalog",
                                "schemaName": "my_schema",
                                "functionName": "function_one"
                            }
                        },
                        {
                            "function": {
                                "catalogName": "my_catalog",
                                "schemaName": "my_schema",
                                "functionName": "function_two"
                            }
                        },
                        {
                            "function": {
                                "catalogName": "my_catalog",
                                "schemaName": "my_schema",
                                "functionName": "function_three"
                            }
                        }
                    ]
                }""";
        assertStringRequestsEqual(ImmutableList.of(expectedRequest), this.mockClient.getRequests(), "/input/action");
    }

    @Test
    public void testEmptyFilterColumns()
    {
        SchemaTableName tableOne = SchemaTableName.schemaTableName("my_schema", "table_one");
        SchemaTableName tableTwo = SchemaTableName.schemaTableName("my_schema", "table_two");
        Map<SchemaTableName, Set<String>> requestedColumns = ImmutableMap.<SchemaTableName, Set<String>>builder()
                .put(tableOne, ImmutableSet.of())
                .put(tableTwo, ImmutableSet.of())
                .buildOrThrow();

        Map<SchemaTableName, Set<String>> result = authorizer.filterColumns(
                requestingSecurityContext,
                "my_catalog",
                requestedColumns);
        assertTrue(mockClient.getRequests().isEmpty());
        assertTrue(Maps.difference(result, requestedColumns).areEqual());
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.FilteringTestHelpers#emptyInputTestCases")
    public void testEmptyRequests(
            BiFunction<OpaAccessControl, SystemSecurityContext, Collection> callable)
    {
        Collection result = callable.apply(authorizer, requestingSecurityContext);
        assertTrue(result.isEmpty());
        assertTrue(mockClient.getRequests().isEmpty());
    }

    @ParameterizedTest(name = "{index}: {0} - {1}")
    @MethodSource("io.trino.plugin.opa.FilteringTestHelpers#prepopulatedErrorCases")
    public void testIllegalResponseThrows(
            BiFunction<OpaAccessControl, SystemSecurityContext, ?> callable,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> callable.apply(authorizer, requestingSecurityContext));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
        assertFalse(mockClient.getRequests().isEmpty());
    }

    private ObjectNode encodeObjectWithKey(Object inp, String key)
    {
        return jsonMapper.createObjectNode().set(key, jsonMapper.valueToTree(inp));
    }

    private static Stream<Arguments> subsetProvider()
    {
        return Stream.of(
                Arguments.of(Named.of("All-3-resources", new HttpClientUtils.MockResponse("{\"result\": [0, 1, 2]}", 200)), ImmutableList.of(0, 1, 2)),
                Arguments.of(Named.of("First-and-last-resources", new HttpClientUtils.MockResponse("{\"result\": [0, 2]}", 200)), ImmutableList.of(0, 2)),
                Arguments.of(Named.of("Only-one-resource", new HttpClientUtils.MockResponse("{\"result\": [2]}", 200)), ImmutableList.of(2)),
                Arguments.of(Named.of("No-resources", new HttpClientUtils.MockResponse("{\"result\": []}", 200)), ImmutableList.of()));
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
}
