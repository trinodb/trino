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
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.opa.RequestTestUtilities.assertStringRequestsEqual;
import static io.trino.plugin.opa.RequestTestUtilities.buildValidatingRequestHandler;
import static io.trino.plugin.opa.TestConstants.NO_ACCESS_RESPONSE;
import static io.trino.plugin.opa.TestConstants.OK_RESPONSE;
import static io.trino.plugin.opa.TestConstants.OPA_SERVER_URI;
import static io.trino.plugin.opa.TestConstants.TEST_IDENTITY;
import static io.trino.plugin.opa.TestConstants.TEST_SECURITY_CONTEXT;
import static io.trino.plugin.opa.TestConstants.simpleOpaConfig;
import static io.trino.plugin.opa.TestHelpers.assertAccessControlMethodThrowsForIllegalResponses;
import static io.trino.plugin.opa.TestHelpers.createMockHttpClient;
import static io.trino.plugin.opa.TestHelpers.createOpaAuthorizer;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpaAccessControlFiltering
{
    @Test
    public void testFilterViewQueryOwnedBy()
    {
        Identity userOne = Identity.ofUser("user-one");
        Identity userTwo = Identity.ofUser("user-two");
        List<Identity> requestedIdentities = ImmutableList.of(userOne, userTwo);

        assertAccessControlMethodThrowsForIllegalResponses(
                authorizer -> authorizer.filterViewQueryOwnedBy(TEST_IDENTITY, requestedIdentities),
                simpleOpaConfig(),
                OPA_SERVER_URI);
        assertFilteringAccessControlMethodDoesNotSendRequests(
                authorizer -> authorizer.filterViewQueryOwnedBy(TEST_IDENTITY, ImmutableList.of()));

        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildHandler("/input/action/resource/user/user", "user-one"));
        OpaAccessControl authorizer = createOpaAuthorizer(simpleOpaConfig(), mockClient);
        Collection<Identity> result = authorizer.filterViewQueryOwnedBy(
                TEST_IDENTITY,
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
        Set<String> requestedCatalogs = ImmutableSet.of("catalog_one", "catalog_two");
        assertAccessControlMethodThrowsForIllegalResponses(
                authorizer -> authorizer.filterCatalogs(TEST_SECURITY_CONTEXT, requestedCatalogs),
                simpleOpaConfig(),
                OPA_SERVER_URI);
        assertFilteringAccessControlMethodDoesNotSendRequests(
                authorizer -> authorizer.filterCatalogs(TEST_SECURITY_CONTEXT, ImmutableSet.of()));

        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildHandler("/input/action/resource/catalog/name", "catalog_two"));
        OpaAccessControl authorizer = createOpaAuthorizer(simpleOpaConfig(), mockClient);
        Set<String> result = authorizer.filterCatalogs(
                TEST_SECURITY_CONTEXT,
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
        Set<String> requestedSchemas = ImmutableSet.of("schema_one", "schema_two");
        assertAccessControlMethodThrowsForIllegalResponses(
                authorizer -> authorizer.filterSchemas(TEST_SECURITY_CONTEXT, "some_catalog", requestedSchemas),
                simpleOpaConfig(),
                OPA_SERVER_URI);
        assertFilteringAccessControlMethodDoesNotSendRequests(
                authorizer -> authorizer.filterSchemas(TEST_SECURITY_CONTEXT, "some_catalog", ImmutableSet.of()));

        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildHandler("/input/action/resource/schema/schemaName", "schema_one"));
        OpaAccessControl authorizer = createOpaAuthorizer(simpleOpaConfig(), mockClient);
        Set<String> result = authorizer.filterSchemas(
                TEST_SECURITY_CONTEXT,
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
        assertAccessControlMethodThrowsForIllegalResponses(
                authorizer -> authorizer.filterTables(TEST_SECURITY_CONTEXT, "some_catalog", tables),
                simpleOpaConfig(),
                OPA_SERVER_URI);
        assertFilteringAccessControlMethodDoesNotSendRequests(
                authorizer -> authorizer.filterTables(TEST_SECURITY_CONTEXT, "some_catalog", ImmutableSet.of()));

        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildHandler("/input/action/resource/table/tableName", "table_one"));
        OpaAccessControl authorizer = createOpaAuthorizer(simpleOpaConfig(), mockClient);

        Set<SchemaTableName> result = authorizer.filterTables(TEST_SECURITY_CONTEXT, "my_catalog", tables);
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
        assertAccessControlMethodThrowsForIllegalResponses(
                authorizer -> authorizer.filterColumns(TEST_SECURITY_CONTEXT, "some_catalog", requestedColumns),
                simpleOpaConfig(),
                OPA_SERVER_URI);
        assertFilteringAccessControlMethodDoesNotSendRequests(
                authorizer -> authorizer.filterColumns(TEST_SECURITY_CONTEXT, "some_catalog", ImmutableMap.of()).entrySet());
        assertFilteringAccessControlMethodDoesNotSendRequests(
                authorizer -> authorizer.filterColumns(
                        TEST_SECURITY_CONTEXT,
                        "some_catalog",
                        ImmutableMap.<SchemaTableName, Set<String>>builder()
                                .put(tableOne, ImmutableSet.of())
                                .put(tableTwo, ImmutableSet.of())
                                .put(tableThree, ImmutableSet.of())
                                .buildOrThrow()).entrySet());

        // Allow both columns from one table, one column from another one and no columns from the last one
        Set<String> columnsToAllow = ImmutableSet.<String>builder()
                .add("table_one_column_one")
                .add("table_one_column_two")
                .add("table_two_column_two")
                .build();

        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildHandler("/input/action/resource/table/columns/0", columnsToAllow));
        OpaAccessControl authorizer = createOpaAuthorizer(simpleOpaConfig(), mockClient);

        Map<SchemaTableName, Set<String>> result = authorizer.filterColumns(TEST_SECURITY_CONTEXT, "my_catalog", requestedColumns);

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
    public void testFilterFunctions()
    {
        SchemaFunctionName functionOne = new SchemaFunctionName("my_schema", "function_one");
        SchemaFunctionName functionTwo = new SchemaFunctionName("my_schema", "function_two");
        Set<SchemaFunctionName> requestedFunctions = ImmutableSet.of(functionOne, functionTwo);
        assertAccessControlMethodThrowsForIllegalResponses(
                authorizer -> authorizer.filterFunctions(TEST_SECURITY_CONTEXT, "some_catalog", requestedFunctions),
                simpleOpaConfig(),
                OPA_SERVER_URI);
        assertFilteringAccessControlMethodDoesNotSendRequests(
                authorizer -> authorizer.filterFunctions(TEST_SECURITY_CONTEXT, "some_catalog", ImmutableSet.of()));

        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildHandler("/input/action/resource/function/functionName", "function_two"));
        OpaAccessControl authorizer = createOpaAuthorizer(simpleOpaConfig(), mockClient);

        Set<SchemaFunctionName> result = authorizer.filterFunctions(
                TEST_SECURITY_CONTEXT,
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

    private static void assertFilteringAccessControlMethodDoesNotSendRequests(Function<OpaAccessControl, Collection<?>> method)
    {
        InstrumentedHttpClient httpClientForEmptyRequest = createMockHttpClient(OPA_SERVER_URI, request -> OK_RESPONSE);
        assertThat(method.apply(createOpaAuthorizer(simpleOpaConfig(), httpClientForEmptyRequest))).isEmpty();
        assertThat(httpClientForEmptyRequest.getRequests()).isEmpty();
    }

    private static Function<JsonNode, MockResponse> buildHandler(String jsonPath, Set<String> resourcesToAccept)
    {
        return buildValidatingRequestHandler(TEST_IDENTITY, parsedRequest -> {
            String requestedItem = parsedRequest.at(jsonPath).asText();
            if (resourcesToAccept.contains(requestedItem)) {
                return OK_RESPONSE;
            }
            return NO_ACCESS_RESPONSE;
        });
    }

    private static Function<JsonNode, MockResponse> buildHandler(String jsonPath, String resourceToAccept)
    {
        return buildHandler(jsonPath, ImmutableSet.of(resourceToAccept));
    }
}
