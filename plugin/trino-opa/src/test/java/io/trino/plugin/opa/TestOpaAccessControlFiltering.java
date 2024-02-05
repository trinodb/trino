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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.opa.HttpClientUtils.InstrumentedHttpClient;
import io.trino.plugin.opa.HttpClientUtils.MockResponse;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.opa.RequestTestUtilities.assertStringRequestsEqual;
import static io.trino.plugin.opa.RequestTestUtilities.buildValidatingRequestHandler;
import static io.trino.plugin.opa.TestHelpers.NO_ACCESS_RESPONSE;
import static io.trino.plugin.opa.TestHelpers.OK_RESPONSE;
import static io.trino.plugin.opa.TestHelpers.createMockHttpClient;
import static io.trino.plugin.opa.TestHelpers.createOpaAuthorizer;
import static io.trino.plugin.opa.TestHelpers.systemSecurityContextFromIdentity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOpaAccessControlFiltering
{
    private static final URI OPA_SERVER_URI = URI.create("http://my-uri/");
    private final Identity requestingIdentity = Identity.ofUser("source-user");
    private final SystemSecurityContext requestingSecurityContext = systemSecurityContextFromIdentity(requestingIdentity);

    @Test
    public void testFilterViewQueryOwnedBy()
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildHandler("/input/action/resource/user/user", "user-one"));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, mockClient);

        Identity userOne = Identity.ofUser("user-one");
        Identity userTwo = Identity.ofUser("user-two");
        List<Identity> requestedIdentities = ImmutableList.of(userOne, userTwo);

        Collection<Identity> result = authorizer.filterViewQueryOwnedBy(
                requestingIdentity,
                requestedIdentities);
        assertThat(result).containsExactly(userOne);

        Set<String> expectedRequests = ImmutableSet.<String>builder()
                .add("""
                    {
                        "operation": "FilterViewQueryOwnedBy",
                        "resource": {
                            "user": {
                                "user": "user-one",
                                "groups": []
                            }
                        }
                    }
                    """)
                .add("""
                    {
                        "operation": "FilterViewQueryOwnedBy",
                        "resource": {
                            "user": {
                                "user": "user-two",
                                "groups": []
                            }
                        }
                    }
                    """)
                .build();
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
    }

    @Test
    public void testFilterCatalogs()
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildHandler("/input/action/resource/catalog/name", "catalog_two"));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, mockClient);

        Set<String> requestedCatalogs = ImmutableSet.of("catalog_one", "catalog_two");
        Set<String> result = authorizer.filterCatalogs(
                requestingSecurityContext,
                requestedCatalogs);
        assertThat(result).containsExactly("catalog_two");

        Set<String> expectedRequests = ImmutableSet.<String>builder()
                .add("""
                    {
                        "operation": "FilterCatalogs",
                        "resource": {
                            "catalog": {
                                "name": "catalog_one"
                            }
                        }
                    }
                    """)
                .add("""
                    {
                        "operation": "FilterCatalogs",
                        "resource": {
                            "catalog": {
                                "name": "catalog_two"
                            }
                        }
                    }
                    """)
                .build();
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
    }

    @Test
    public void testFilterSchemas()
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildHandler("/input/action/resource/schema/schemaName", "schema_one"));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, mockClient);

        Set<String> requestedSchemas = ImmutableSet.of("schema_one", "schema_two");

        Set<String> result = authorizer.filterSchemas(
                requestingSecurityContext,
                "my_catalog",
                requestedSchemas);
        assertThat(result).containsExactly("schema_one");

        Set<String> expectedRequests = requestedSchemas.stream()
                .map("""
                    {
                        "operation": "FilterSchemas",
                        "resource": {
                            "schema": {
                                "schemaName": "%s",
                                "catalogName": "my_catalog"
                            }
                        }
                    }
                    """::formatted)
                .collect(toImmutableSet());
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
    }

    @Test
    public void testFilterTables()
    {
        Set<SchemaTableName> tables = ImmutableSet.<SchemaTableName>builder()
                .add(new SchemaTableName("schema_one", "table_one"))
                .add(new SchemaTableName("schema_one", "table_two"))
                .add(new SchemaTableName("schema_two", "table_one"))
                .add(new SchemaTableName("schema_two", "table_two"))
                .build();
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildHandler("/input/action/resource/table/tableName", "table_one"));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, mockClient);

        Set<SchemaTableName> result = authorizer.filterTables(requestingSecurityContext, "my_catalog", tables);
        assertThat(result).containsExactlyInAnyOrderElementsOf(tables.stream().filter(table -> table.getTableName().equals("table_one")).collect(toImmutableSet()));

        Set<String> expectedRequests = tables.stream()
                .map(table -> """
                    {
                        "operation": "FilterTables",
                        "resource": {
                            "table": {
                                "tableName": "%s",
                                "schemaName": "%s",
                                "catalogName": "my_catalog"
                            }
                        }
                    }
                    """.formatted(table.getTableName(), table.getSchemaName()))
                .collect(toImmutableSet());
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
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
        Set<String> columnsToAllow = ImmutableSet.<String>builder()
                .add("table_one_column_one")
                .add("table_one_column_two")
                .add("table_two_column_two")
                .build();

        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildHandler("/input/action/resource/table/columns/0", columnsToAllow));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, mockClient);

        Map<SchemaTableName, Set<String>> result = authorizer.filterColumns(requestingSecurityContext, "my_catalog", requestedColumns);

        Set<String> expectedRequests = requestedColumns.entrySet().stream()
                .<String>mapMulti(
                        (requestedColumnsForTable, accepter) -> requestedColumnsForTable.getValue().forEach(
                                column -> accepter.accept("""
                                        {
                                            "operation": "FilterColumns",
                                            "resource": {
                                                "table": {
                                                    "tableName": "%s",
                                                    "schemaName": "my_schema",
                                                    "catalogName": "my_catalog",
                                                    "columns": ["%s"]
                                                }
                                            }
                                        }
                                        """.formatted(requestedColumnsForTable.getKey().getTableName(), column))))
                .collect(toImmutableSet());
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
        assertThat(result).containsExactlyInAnyOrderEntriesOf(
                ImmutableMap.<SchemaTableName, Set<String>>builder()
                        .put(tableOne, ImmutableSet.of("table_one_column_one", "table_one_column_two"))
                        .put(tableTwo, ImmutableSet.of("table_two_column_two"))
                        .buildOrThrow());
    }

    @Test
    public void testEmptyFilterColumns()
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, request -> OK_RESPONSE);
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, mockClient);

        SchemaTableName someTable = SchemaTableName.schemaTableName("my_schema", "my_table");
        Map<SchemaTableName, Set<String>> requestedColumns = ImmutableMap.of(someTable, ImmutableSet.of());

        Map<SchemaTableName, Set<String>> result = authorizer.filterColumns(
                requestingSecurityContext,
                "my_catalog",
                requestedColumns);

        assertThat(mockClient.getRequests()).isEmpty();
        assertThat(result).isEmpty();
    }

    @Test
    public void testFilterFunctions()
    {
        SchemaFunctionName functionOne = new SchemaFunctionName("my_schema", "function_one");
        SchemaFunctionName functionTwo = new SchemaFunctionName("my_schema", "function_two");
        Set<SchemaFunctionName> requestedFunctions = ImmutableSet.of(functionOne, functionTwo);

        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildHandler("/input/action/resource/function/functionName", "function_two"));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, mockClient);

        Set<SchemaFunctionName> result = authorizer.filterFunctions(
                requestingSecurityContext,
                "my_catalog",
                requestedFunctions);
        assertThat(result).containsExactly(functionTwo);

        Set<String> expectedRequests = requestedFunctions.stream()
                .map(function -> """
                    {
                        "operation": "FilterFunctions",
                        "resource": {
                            "function": {
                                "catalogName": "my_catalog",
                                "schemaName": "%s",
                                "functionName": "%s"
                            }
                        }
                    }""".formatted(function.getSchemaName(), function.getFunctionName()))
                .collect(toImmutableSet());
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.FilteringTestHelpers#emptyInputTestCases")
    public void testEmptyRequests(
            BiFunction<OpaAccessControl, SystemSecurityContext, Collection> callable)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, request -> OK_RESPONSE);
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, mockClient);

        Collection<?> result = callable.apply(authorizer, requestingSecurityContext);
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
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(requestingIdentity, failureResponse));
        OpaAccessControl authorizer = createOpaAuthorizer(OPA_SERVER_URI, mockClient);

        assertThatThrownBy(() -> callable.apply(authorizer, requestingSecurityContext))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
        assertThat(mockClient.getRequests()).hasSize(1);
    }

    private Function<JsonNode, MockResponse> buildHandler(String jsonPath, Set<String> resourcesToAccept)
    {
        return buildValidatingRequestHandler(requestingIdentity, parsedRequest -> {
            String requestedItem = parsedRequest.at(jsonPath).asText();
            if (resourcesToAccept.contains(requestedItem)) {
                return OK_RESPONSE;
            }
            return NO_ACCESS_RESPONSE;
        });
    }

    private Function<JsonNode, MockResponse> buildHandler(String jsonPath, String resourceToAccept)
    {
        return buildHandler(jsonPath, ImmutableSet.of(resourceToAccept));
    }
}
