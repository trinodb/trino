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
import io.trino.plugin.opa.FunctionalHelpers.Pair;
import io.trino.plugin.opa.HttpClientUtils.InstrumentedHttpClient;
import io.trino.plugin.opa.HttpClientUtils.MockResponse;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.opa.RequestTestUtilities.assertStringRequestsEqual;
import static io.trino.plugin.opa.RequestTestUtilities.buildValidatingRequestHandler;
import static io.trino.plugin.opa.TestConstants.OK_RESPONSE;
import static io.trino.plugin.opa.TestConstants.OPA_SERVER_BATCH_URI;
import static io.trino.plugin.opa.TestConstants.TEST_IDENTITY;
import static io.trino.plugin.opa.TestConstants.TEST_SECURITY_CONTEXT;
import static io.trino.plugin.opa.TestConstants.batchFilteringOpaConfig;
import static io.trino.plugin.opa.TestHelpers.assertAccessControlMethodThrowsForIllegalResponses;
import static io.trino.plugin.opa.TestHelpers.assertAccessControlMethodThrowsForResponse;
import static io.trino.plugin.opa.TestHelpers.createMockHttpClient;
import static io.trino.plugin.opa.TestHelpers.createOpaAuthorizer;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpaBatchAccessControlFiltering
{
    @Test
    public void testFilterViewQueryOwnedBy()
    {
        Identity identityOne = Identity.ofUser("user-one");
        Identity identityTwo = Identity.ofUser("user-two");
        Identity identityThree = Identity.ofUser("user-three");

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
        assertAccessControlMethodBehaviour(
                (accessControl, systemSecurityContext, identities) -> accessControl.filterViewQueryOwnedBy(systemSecurityContext.getIdentity(), identities),
                identityOne,
                identityTwo,
                identityThree,
                ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testFilterCatalogs()
    {
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
        assertAccessControlMethodBehaviour(
                OpaAccessControl::filterCatalogs, "catalog_one", "catalog_two", "catalog_three", ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testFilterSchemas()
    {
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
        assertAccessControlMethodBehaviour(
                (accessControl, systemSecurityContext, items) -> accessControl.filterSchemas(systemSecurityContext, "my_catalog", items),
                "schema_one",
                "schema_two",
                "schema_three",
                ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testFilterTables()
    {
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
        assertAccessControlMethodBehaviour(
                (accessControl, systemSecurityContext, items) -> accessControl.filterTables(systemSecurityContext, "my_catalog", items),
                new SchemaTableName("schema_one", "table_one"),
                new SchemaTableName("schema_one", "table_two"),
                new SchemaTableName("schema_two", "table_one"),
                ImmutableSet.of(expectedRequest));
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
                OPA_SERVER_BATCH_URI,
                buildValidatingRequestHandler(
                        TEST_IDENTITY,
                        parsedRequest -> {
                            String tableName = parsedRequest.at("/input/action/filterResources/0/table/tableName").asText();
                            String responseContents = switch(tableName) {
                                case "table_one" -> "{\"result\": [0, 1]}";
                                case "table_two" -> "{\"result\": [1]}";
                                default -> "{\"result\": []}";
                            };
                            return new MockResponse(responseContents, 200);
                        }));
        OpaAccessControl authorizer = createOpaAuthorizer(batchFilteringOpaConfig(), mockClient);
        Map<SchemaTableName, Set<String>> result = authorizer.filterColumns(
                TEST_SECURITY_CONTEXT,
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

    @Test
    public void testEmptyFilterColumns()
    {
        assertFilteringAccessControlMethodDoesNotSendRequests(
                accessControl -> accessControl.filterColumns(TEST_SECURITY_CONTEXT, "my_catalog", ImmutableMap.of()).entrySet());

        SchemaTableName tableOne = SchemaTableName.schemaTableName("my_schema", "table_one");
        SchemaTableName tableTwo = SchemaTableName.schemaTableName("my_schema", "table_two");
        Map<SchemaTableName, Set<String>> requestedColumns = ImmutableMap.<SchemaTableName, Set<String>>builder()
                .put(tableOne, ImmutableSet.of())
                .put(tableTwo, ImmutableSet.of())
                .buildOrThrow();
        assertFilteringAccessControlMethodDoesNotSendRequests(
                accessControl -> accessControl.filterColumns(TEST_SECURITY_CONTEXT, "my_catalog", requestedColumns).entrySet());
    }

    @Test
    public void testFilterColumnErrorCases()
    {
        assertAccessControlMethodThrowsForIllegalResponses(
                accessControl -> accessControl.filterColumns(
                        TEST_SECURITY_CONTEXT,
                        "my_catalog",
                        ImmutableMap.of(new SchemaTableName("some_schema", "some_table"), ImmutableSet.of("column_one", "column_two"))),
                batchFilteringOpaConfig(),
                OPA_SERVER_BATCH_URI);
    }

    @Test
    public void testFilterFunctions()
    {
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
        assertAccessControlMethodBehaviour(
                (authorizer, systemSecurityContext, items) -> authorizer.filterFunctions(systemSecurityContext, "my_catalog", items),
                new SchemaFunctionName("my_schema", "function_one"),
                new SchemaFunctionName("my_schema", "function_two"),
                new SchemaFunctionName("my_schema", "function_three"),
                ImmutableSet.of(expectedRequest));
    }

    private static <T> void assertAccessControlMethodBehaviour(
            FunctionalHelpers.Function3<OpaAccessControl, SystemSecurityContext, Set<T>, Collection<T>> method, T obj1, T obj2, T obj3, Set<String> expectedRequests)
    {
        assertFilteringAccessControlMethodDoesNotSendRequests(accessControl -> method.apply(accessControl, TEST_SECURITY_CONTEXT, ImmutableSet.of()));

        Set<T> objectsToFilter = ImmutableSet.of(obj1, obj2, obj3);
        List<Pair<String, List<T>>> responsesAndExpectedFilteredObjects = ImmutableList.<Pair<String, List<T>>>builder()
                .add(Pair.of("{\"result\": []}", ImmutableList.of()))
                .add(Pair.of("{\"result\": [0]}", ImmutableList.of(obj1)))
                .add(Pair.of("{\"result\": [1]}", ImmutableList.of(obj2)))
                .add(Pair.of("{\"result\": [2]}", ImmutableList.of(obj3)))
                .add(Pair.of("{\"result\": [0, 2]}", ImmutableList.of(obj1, obj3)))
                .add(Pair.of("{\"result\": [1, 2]}", ImmutableList.of(obj2, obj3)))
                .add(Pair.of("{\"result\": [0, 1, 2]}", ImmutableList.of(obj1, obj2, obj3)))
                .build();
        for (Pair<String, List<T>> testCase : responsesAndExpectedFilteredObjects) {
            InstrumentedHttpClient httpClient = createMockHttpClient(OPA_SERVER_BATCH_URI, buildValidatingRequestHandler(TEST_IDENTITY, 200, testCase.first()));
            OpaAccessControl accessControl = createOpaAuthorizer(batchFilteringOpaConfig(), httpClient);
            assertThat(method.apply(accessControl, TEST_SECURITY_CONTEXT, objectsToFilter)).containsExactlyInAnyOrderElementsOf(testCase.second());
            assertStringRequestsEqual(expectedRequests, httpClient.getRequests(), "/input/action");
        }

        Consumer<OpaAccessControl> methodWithItems = accessControl -> method.apply(accessControl, TEST_SECURITY_CONTEXT, objectsToFilter);
        assertAccessControlMethodThrowsForIllegalResponses(methodWithItems, batchFilteringOpaConfig(), OPA_SERVER_BATCH_URI);
        assertAccessControlMethodThrowsForResponse(
                new MockResponse("{\"result\": [0, 1, 2, 3]}", 200),
                OPA_SERVER_BATCH_URI,
                batchFilteringOpaConfig(),
                methodWithItems,
                OpaQueryException.QueryFailed.class,
                "Failed to query OPA backend");
    }

    private static void assertFilteringAccessControlMethodDoesNotSendRequests(Function<OpaAccessControl, Collection<?>> method)
    {
        InstrumentedHttpClient httpClientForEmptyRequest = createMockHttpClient(OPA_SERVER_BATCH_URI, request -> OK_RESPONSE);
        assertThat(method.apply(createOpaAuthorizer(batchFilteringOpaConfig(), httpClientForEmptyRequest))).isEmpty();
        assertThat(httpClientForEmptyRequest.getRequests()).isEmpty();
    }
}
