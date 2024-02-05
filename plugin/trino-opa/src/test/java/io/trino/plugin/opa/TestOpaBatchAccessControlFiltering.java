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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.opa.HttpClientUtils.InstrumentedHttpClient;
import io.trino.plugin.opa.HttpClientUtils.MockResponse;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.opa.RequestTestUtilities.assertStringRequestsEqual;
import static io.trino.plugin.opa.RequestTestUtilities.buildValidatingRequestHandler;
import static io.trino.plugin.opa.TestHelpers.OK_RESPONSE;
import static io.trino.plugin.opa.TestHelpers.createMockHttpClient;
import static io.trino.plugin.opa.TestHelpers.createOpaAuthorizer;
import static io.trino.plugin.opa.TestHelpers.systemSecurityContextFromIdentity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOpaBatchAccessControlFiltering
{
    private static final URI OPA_SERVER_URI = URI.create("http://my-uri/");
    private static final URI OPA_BATCH_SERVER_URI = URI.create("http://my-uri/batchAllow");
    private final Identity requestingIdentity = Identity.ofUser("source-user");
    private final SystemSecurityContext requestingSecurityContext = systemSecurityContextFromIdentity(requestingIdentity);

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestOpaBatchAccessControlFiltering#subsetProvider")
    public void testFilterViewQueryOwnedBy(
            MockResponse response,
            List<Integer> expectedItems)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_BATCH_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, response));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, OPA_BATCH_SERVER_URI, mockClient);

        Identity identityOne = Identity.ofUser("user-one");
        Identity identityTwo = Identity.ofUser("user-two");
        Identity identityThree = Identity.ofUser("user-three");
        List<Identity> requestedIdentities = ImmutableList.of(identityOne, identityTwo, identityThree);

        Collection<Identity> result = authorizer.filterViewQueryOwnedBy(requestingIdentity, requestedIdentities);
        assertThat(result).containsExactlyInAnyOrderElementsOf(getSubset(requestedIdentities, expectedItems));

        String expectedRequest = """
                {
                    "operation": "FilterViewQueryOwnedBy",
                    "filterResources": [
                        {
                            "user": {
                                "user": "user-one",
                                "groups": []
                            }
                        },
                        {
                            "user": {
                                "user": "user-two",
                                "groups": []
                            }
                        },
                        {
                            "user": {
                                "user": "user-three",
                                "groups": []
                            }
                        }
                    ]
                }""";
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestOpaBatchAccessControlFiltering#subsetProvider")
    public void testFilterCatalogs(
            MockResponse response,
            List<Integer> expectedItems)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_BATCH_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, response));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, OPA_BATCH_SERVER_URI, mockClient);

        List<String> requestedCatalogs = ImmutableList.of("catalog_one", "catalog_two", "catalog_three");

        Set<String> result = authorizer.filterCatalogs(
                requestingSecurityContext,
                new LinkedHashSet<>(requestedCatalogs));
        assertThat(result).containsExactlyInAnyOrderElementsOf(getSubset(requestedCatalogs, expectedItems));

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
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestOpaBatchAccessControlFiltering#subsetProvider")
    public void testFilterSchemas(
            MockResponse response,
            List<Integer> expectedItems)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_BATCH_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, response));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, OPA_BATCH_SERVER_URI, mockClient);
        List<String> requestedSchemas = ImmutableList.of("schema_one", "schema_two", "schema_three");

        Set<String> result = authorizer.filterSchemas(
                requestingSecurityContext,
                "my_catalog",
                new LinkedHashSet<>(requestedSchemas));
        assertThat(result).containsExactlyInAnyOrderElementsOf(getSubset(requestedSchemas, expectedItems));

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
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestOpaBatchAccessControlFiltering#subsetProvider")
    public void testFilterTables(
            MockResponse response,
            List<Integer> expectedItems)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_BATCH_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, response));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, OPA_BATCH_SERVER_URI, mockClient);
        List<SchemaTableName> tables = ImmutableList.<SchemaTableName>builder()
                .add(new SchemaTableName("schema_one", "table_one"))
                .add(new SchemaTableName("schema_one", "table_two"))
                .add(new SchemaTableName("schema_two", "table_one"))
                .build();

        Set<SchemaTableName> result = authorizer.filterTables(
                requestingSecurityContext,
                "my_catalog",
                new LinkedHashSet<>(tables));
        assertThat(result).containsExactlyInAnyOrderElementsOf(getSubset(tables, expectedItems));

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
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Function<String, MockResponse> buildHandler(Function<String, String> dataBuilder)
    {
        return request -> new MockResponse(dataBuilder.apply(request), 200);
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
        InstrumentedHttpClient mockClient = createMockHttpClient(
                OPA_BATCH_SERVER_URI,
                buildValidatingRequestHandler(
                        requestingIdentity,
                        parsedRequest -> {
                            String tableName = parsedRequest.at("/input/action/filterResources/0/table/tableName").asText();
                            String responseContents = switch(tableName) {
                                case "table_one" -> "{\"result\": [0, 1]}";
                                case "table_two" -> "{\"result\": [1]}";
                                default -> "{\"result\": []}";
                            };
                            return new MockResponse(responseContents, 200);
                        }));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, OPA_BATCH_SERVER_URI, mockClient);
        Map<SchemaTableName, Set<String>> result = authorizer.filterColumns(
                requestingSecurityContext,
                "my_catalog",
                requestedColumns);

        Set<String> expectedRequests = Stream.of("table_one", "table_two", "table_three")
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
                .collect(toImmutableSet());
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
        assertThat(result).containsExactlyInAnyOrderEntriesOf(
                ImmutableMap.<SchemaTableName, Set<String>>builder()
                        .put(tableOne, ImmutableSet.of("table_one_column_one", "table_one_column_two"))
                        .put(tableTwo, ImmutableSet.of("table_two_column_two"))
                        .buildOrThrow());
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestOpaBatchAccessControlFiltering#subsetProvider")
    public void testFilterFunctions(
            MockResponse response,
            List<Integer> expectedItems)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_BATCH_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, response));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, OPA_BATCH_SERVER_URI, mockClient);
        List<SchemaFunctionName> requestedFunctions = ImmutableList.<SchemaFunctionName>builder()
                .add(new SchemaFunctionName("my_schema", "function_one"))
                .add(new SchemaFunctionName("my_schema", "function_two"))
                .add(new SchemaFunctionName("my_schema", "function_three"))
                .build();

        Set<SchemaFunctionName> result = authorizer.filterFunctions(
                requestingSecurityContext,
                "my_catalog",
                new LinkedHashSet<>(requestedFunctions));
        assertThat(result).containsExactlyInAnyOrderElementsOf(getSubset(requestedFunctions, expectedItems));

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
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @Test
    public void testEmptyFilterColumns()
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_BATCH_SERVER_URI, request -> OK_RESPONSE);
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, OPA_BATCH_SERVER_URI, mockClient);

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
        assertThat(mockClient.getRequests()).isEmpty();
        assertThat(result).isEmpty();
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.FilteringTestHelpers#emptyInputTestCases")
    public void testEmptyRequests(
            BiFunction<OpaAccessControl, SystemSecurityContext, Collection> callable)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_BATCH_SERVER_URI, request -> OK_RESPONSE);
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, OPA_BATCH_SERVER_URI, mockClient);

        Collection result = callable.apply(authorizer, requestingSecurityContext);
        assertThat(result).isEmpty();
        assertThat(mockClient.getRequests()).isEmpty();
    }

    @ParameterizedTest(name = "{index}: {0} - {1}")
    @MethodSource("io.trino.plugin.opa.FilteringTestHelpers#prepopulatedErrorCases")
    public void testIllegalResponseThrows(
            BiFunction<OpaAccessControl, SystemSecurityContext, ?> callable,
            MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_BATCH_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, OPA_BATCH_SERVER_URI, mockClient);

        assertThatThrownBy(() -> callable.apply(authorizer, requestingSecurityContext))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
        assertThat(mockClient.getRequests()).isNotEmpty();
    }

    @Test
    public void testResponseOutOfBoundsThrows()
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_BATCH_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, 200, "{\"result\": [0, 1, 2]}"));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, OPA_BATCH_SERVER_URI, mockClient);

        assertThatThrownBy(() -> authorizer.filterCatalogs(requestingSecurityContext, ImmutableSet.of("catalog_one", "catalog_two")))
                .isInstanceOf(OpaQueryException.QueryFailed.class);
        assertThatThrownBy(() -> authorizer.filterSchemas(requestingSecurityContext, "some_catalog", ImmutableSet.of("schema_one", "schema_two")))
                .isInstanceOf(OpaQueryException.QueryFailed.class);
        assertThatThrownBy(() -> authorizer.filterTables(
                requestingSecurityContext,
                "some_catalog",
                        ImmutableSet.of(
                                new SchemaTableName("some_schema", "table_one"),
                                new SchemaTableName("some_schema", "table_two"))))
                .isInstanceOf(OpaQueryException.QueryFailed.class);
        assertThatThrownBy(() -> authorizer.filterColumns(
                requestingSecurityContext,
                "some_catalog",
                ImmutableMap.<SchemaTableName, Set<String>>builder()
                        .put(new SchemaTableName("some_schema", "some_table"), ImmutableSet.of("column_one", "column_two"))
                        .buildOrThrow()))
                .isInstanceOf(OpaQueryException.QueryFailed.class);
        assertThatThrownBy(() -> authorizer.filterViewQueryOwnedBy(
                requestingIdentity,
                ImmutableSet.of(Identity.ofUser("identity_one"), Identity.ofUser("identity_two"))))
                .isInstanceOf(OpaQueryException.QueryFailed.class);
    }

    private static Stream<Arguments> subsetProvider()
    {
        return Stream.of(
                Arguments.of(Named.of("All-3-resources", new MockResponse("{\"result\": [0, 1, 2]}", 200)), ImmutableList.of(0, 1, 2)),
                Arguments.of(Named.of("First-and-last-resources", new MockResponse("{\"result\": [0, 2]}", 200)), ImmutableList.of(0, 2)),
                Arguments.of(Named.of("Only-one-resource", new MockResponse("{\"result\": [2]}", 200)), ImmutableList.of(2)),
                Arguments.of(Named.of("No-resources", new MockResponse("{\"result\": []}", 200)), ImmutableList.of()));
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
