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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemSecurityContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.trino.plugin.openpolicyagent.RequestTestUtilities.assertStringRequestsEqual;
import static io.trino.plugin.openpolicyagent.TestHelpers.NO_ACCESS_RESPONSE;
import static io.trino.plugin.openpolicyagent.TestHelpers.OK_RESPONSE;
import static io.trino.plugin.openpolicyagent.TestHelpers.systemSecurityContextFromIdentity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class OpaAccessControlFilteringUnitTest
{
    private static final URI OPA_SERVER_URI = URI.create("http://my-uri/");
    private HttpClientUtils.InstrumentedHttpClient mockClient;
    private OpaAccessControl authorizer;
    private final JsonMapper jsonMapper = new JsonMapper();
    private Identity requestingIdentity;
    private SystemSecurityContext requestingSecurityContext;

    @BeforeEach
    public void setupAuthorizer()
    {
        this.mockClient = new HttpClientUtils.InstrumentedHttpClient(OPA_SERVER_URI, "POST", JSON_UTF_8.toString(), request -> OK_RESPONSE);
        this.authorizer = (OpaAccessControl) new OpaAccessControlFactory().create(ImmutableMap.of("opa.policy.uri", OPA_SERVER_URI.toString()), Optional.of(mockClient));
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

    @Test
    public void testFilterViewQueryOwnedBy()
    {
        Identity userOne = Identity.ofUser("user-one");
        Identity userTwo = Identity.ofUser("user-two");
        List<Identity> requestedIdentities = ImmutableList.of(userOne, userTwo);
        this.mockClient.setHandler(buildHandler("/input/action/resource/user/name", "user-one"));

        Collection<Identity> result = authorizer.filterViewQueryOwnedBy(
                requestingIdentity,
                requestedIdentities);
        assertEquals(ImmutableSet.copyOf(result), ImmutableSet.of(userOne));

        List<String> expectedRequests = ImmutableList.<String>builder()
                .add("""
                    {
                        "operation": "FilterViewQueryOwnedBy",
                        "resource": {
                            "user": {
                                "name": "user-one",
                                "user": "user-one",
                                "groups": [],
                                "enabledRoles": [],
                                "catalogRoles": {},
                                "extraCredentials": {}
                            }
                        }
                    }
                    """)
                .add("""
                    {
                        "operation": "FilterViewQueryOwnedBy",
                        "resource": {
                            "user": {
                                "name": "user-two",
                                "user": "user-two",
                                "groups": [],
                                "enabledRoles": [],
                                "catalogRoles": {},
                                "extraCredentials": {}
                            }
                        }
                    }
                    """)
                .build();
        assertStringRequestsEqual(expectedRequests, this.mockClient.getRequests(), "/input/action");
    }

    @Test
    public void testFilterCatalogs()
    {
        Set<String> requestedCatalogs = ImmutableSet.of("catalog_one", "catalog_two");
        this.mockClient.setHandler(buildHandler("/input/action/resource/catalog/name", "catalog_two"));

        Set<String> result = authorizer.filterCatalogs(
                requestingSecurityContext,
                requestedCatalogs);
        assertEquals(ImmutableSet.copyOf(result), ImmutableSet.of("catalog_two"));

        List<String> expectedRequests = ImmutableList.<String>builder()
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
        assertStringRequestsEqual(expectedRequests, this.mockClient.getRequests(), "/input/action");
    }

    @Test
    public void testFilterSchemas()
    {
        Set<String> requestedSchemas = ImmutableSet.of("schema_one", "schema_two");
        this.mockClient.setHandler(buildHandler("/input/action/resource/schema/schemaName", "schema_one"));

        Set<String> result = authorizer.filterSchemas(
                requestingSecurityContext,
                "my_catalog",
                requestedSchemas);
        assertEquals(ImmutableSet.copyOf(result), ImmutableSet.of("schema_one"));

        List<String> expectedRequests = requestedSchemas.stream()
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
                .collect(toImmutableList());
        assertStringRequestsEqual(expectedRequests, this.mockClient.getRequests(), "/input/action");
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
        this.mockClient.setHandler(buildHandler("/input/action/resource/table/tableName", "table_one"));

        Set<SchemaTableName> result = authorizer.filterTables(requestingSecurityContext, "my_catalog", tables);
        assertEquals(ImmutableSet.copyOf(result), tables.stream().filter(table -> table.getTableName().equals("table_one")).collect(toImmutableSet()));

        List<String> expectedRequests = tables.stream()
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
                .collect(toImmutableList());
        assertStringRequestsEqual(expectedRequests, this.mockClient.getRequests(), "/input/action");
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

        this.mockClient.setHandler(buildHandler("/input/action/resource/table/columns/0", columnsToAllow));

        Map<SchemaTableName, Set<String>> result = authorizer.filterColumns(requestingSecurityContext, "my_catalog", requestedColumns);

        List<String> expectedRequests = requestedColumns.entrySet().stream()
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
                .collect(toImmutableList());
        assertStringRequestsEqual(expectedRequests, this.mockClient.getRequests(), "/input/action");
        assertTrue(
                Maps.difference(
                        result,
                        ImmutableMap.builder()
                                .put(tableOne, ImmutableSet.of("table_one_column_one", "table_one_column_two"))
                                .put(tableTwo, ImmutableSet.of("table_two_column_two"))
                                .put(tableThree, ImmutableSet.of())
                                .buildOrThrow()).areEqual());
    }

    @Test
    public void testEmptyFilterColumns()
    {
        SchemaTableName someTable = SchemaTableName.schemaTableName("my_schema", "my_table");
        Map<SchemaTableName, Set<String>> requestedColumns = ImmutableMap.of(someTable, ImmutableSet.of());

        Map<SchemaTableName, Set<String>> result = authorizer.filterColumns(
                requestingSecurityContext,
                "my_catalog",
                requestedColumns);

        assertTrue(mockClient.getRequests().isEmpty());
        assertTrue(Maps.difference(result, requestedColumns).areEqual());
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.FilteringTestHelpers#emptyInputTestCases")
    public void testEmptyRequests(
            BiFunction<OpaAccessControl, SystemSecurityContext, Collection> callable)
    {
        Collection result = callable.apply(authorizer, requestingSecurityContext);
        assertTrue(result.isEmpty());
        assertTrue(mockClient.getRequests().isEmpty());
    }

    @ParameterizedTest(name = "{index}: {0} - {1}")
    @MethodSource("io.trino.plugin.openpolicyagent.FilteringTestHelpers#prepopulatedErrorCases")
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
        assertEquals(mockClient.getRequests().size(), 1);
    }

    private Function<String, HttpClientUtils.MockResponse> buildHandler(String jsonPath, Set<String> resourcesToAccept)
    {
        return request -> {
            try {
                JsonNode parsedRequest = this.jsonMapper.readTree(request);
                String requestedItem = parsedRequest.at(jsonPath).asText();
                if (resourcesToAccept.contains(requestedItem)) {
                    return OK_RESPONSE;
                }
            }
            catch (IOException e) {
                fail("Could not parse request");
            }
            return NO_ACCESS_RESPONSE;
        };
    }
    private Function<String, HttpClientUtils.MockResponse> buildHandler(String jsonPath, String resourceToAccept)
    {
        return buildHandler(jsonPath, ImmutableSet.of(resourceToAccept));
    }
}
