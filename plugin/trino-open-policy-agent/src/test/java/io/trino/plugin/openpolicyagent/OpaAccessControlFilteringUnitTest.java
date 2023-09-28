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
import java.util.stream.Collectors;

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
    private static URI opaServerUri = URI.create("http://my-uri/");
    private HttpClientUtils.InstrumentedHttpClient mockClient;
    private OpaAccessControl authorizer;
    private JsonMapper jsonMapper = new JsonMapper();
    private Identity requestingIdentity;
    private SystemSecurityContext requestingSecurityContext;

    @BeforeEach
    public void setupAuthorizer()
    {
        this.mockClient = new HttpClientUtils.InstrumentedHttpClient(opaServerUri, "POST", JSON_UTF_8.toString(), (request) -> OK_RESPONSE);
        this.authorizer = (OpaAccessControl) new OpaAccessControlFactory()
                .create(Map.of("opa.policy.uri", opaServerUri.toString()),
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

    private Function<String, HttpClientUtils.MockResponse> buildHandler(String jsonPath, Set<String> resourcesToAccept)
    {
        return (request) -> {
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
        return buildHandler(jsonPath, Set.of(resourceToAccept));
    }

    @Test
    public void testFilterViewQueryOwnedBy()
    {
        Identity userOne = Identity.ofUser("user-one");
        Identity userTwo = Identity.ofUser("user-two");
        List<Identity> requestedIdentities = List.of(userOne, userTwo);
        this.mockClient.setHandler(buildHandler("/input/action/resource/user/name", "user-one"));

        Collection<Identity> result = authorizer.filterViewQueryOwnedBy(
                requestingIdentity,
                requestedIdentities);
        assertEquals(Set.copyOf(result), Set.of(userOne));

        List<String> expectedRequests = List.of(
                """
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
                        }""",
                """
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
                        }""");
        assertStringRequestsEqual(expectedRequests, this.mockClient.getRequests(), "/input/action");
    }

    @Test
    public void testFilterCatalogs()
    {
        Set<String> requestedCatalogs = Set.of("catalog-one", "catalog-two");
        this.mockClient.setHandler(buildHandler("/input/action/resource/catalog/name", "catalog-two"));

        Set<String> result = authorizer.filterCatalogs(
                requestingSecurityContext,
                requestedCatalogs);
        assertEquals(Set.copyOf(result), Set.of("catalog-two"));

        List<String> expectedRequests = List.of(
                """
                        {
                            "operation": "FilterCatalogs",
                            "resource": {
                                "catalog": {
                                    "name": "catalog-one"
                                }
                            }
                        }""",
                """
                        {
                            "operation": "FilterCatalogs",
                            "resource": {
                                "catalog": {
                                    "name": "catalog-two"
                                }
                            }
                        }""");
        assertStringRequestsEqual(expectedRequests, this.mockClient.getRequests(), "/input/action");
    }

    @Test
    public void testFilterSchemas()
    {
        Set<String> requestedSchemas = Set.of("schema-one", "schema-two");
        this.mockClient.setHandler(buildHandler("/input/action/resource/schema/schemaName", "schema-one"));

        Set<String> result = authorizer.filterSchemas(
                requestingSecurityContext,
                "my-catalog",
                requestedSchemas);
        assertEquals(Set.copyOf(result), Set.of("schema-one"));

        List<String> expectedRequests = requestedSchemas.stream().map(
                        """
                                {
                                    "operation": "FilterSchemas",
                                    "resource": {
                                        "schema": {
                                            "schemaName": "%s",
                                            "catalogName": "my-catalog"
                                        }
                                    }
                                }"""::formatted)
                .collect(Collectors.toList());
        assertStringRequestsEqual(expectedRequests, this.mockClient.getRequests(), "/input/action");
    }

    @Test
    public void testFilterTables()
    {
        Set<SchemaTableName> tables = Set.of(
                new SchemaTableName("schema-one", "table-one"),
                new SchemaTableName("schema-one", "table-two"),
                new SchemaTableName("schema-two", "table-one"),
                new SchemaTableName("schema-two", "table-two"));
        this.mockClient.setHandler(buildHandler("/input/action/resource/table/tableName", "table-one"));

        Set<SchemaTableName> result = authorizer.filterTables(
                requestingSecurityContext,
                "my-catalog",
                tables);
        assertEquals(Set.copyOf(result), tables.stream().filter((i) -> i.getTableName().equals("table-one")).collect(Collectors.toSet()));

        List<String> expectedRequests = tables.stream().map(
                        (i) -> """
                                {
                                    "operation": "FilterTables",
                                    "resource": {
                                        "table": {
                                            "tableName": "%s",
                                            "schemaName": "%s",
                                            "catalogName": "my-catalog"
                                        }
                                    }
                                }""".formatted(i.getTableName(), i.getSchemaName()))
                .collect(Collectors.toList());
        assertStringRequestsEqual(expectedRequests, this.mockClient.getRequests(), "/input/action");
    }

    @Test
    public void testFilterColumns()
    {
        SchemaTableName tableOne = SchemaTableName.schemaTableName("my-schema", "table-one");
        SchemaTableName tableTwo = SchemaTableName.schemaTableName("my-schema", "table-two");
        SchemaTableName tableThree = SchemaTableName.schemaTableName("my-schema", "table-three");
        Map<SchemaTableName, Set<String>> requestedColumns = Map.of(
                tableOne, Set.of("table-one-column-one", "table-one-column-two"),
                tableTwo, Set.of("table-two-column-one", "table-two-column-two"),
                tableThree, Set.of("table-three-column-one", "table-three-column-two"));
        // Allow both columns from one table, one column from another one and no columns from the last one
        Set<String> columnsToAllow = Set.of("table-one-column-one", "table-one-column-two", "table-two-column-two");

        this.mockClient.setHandler(buildHandler("/input/action/resource/table/columns/0", columnsToAllow));

        Map<SchemaTableName, Set<String>> result = authorizer.filterColumns(
                requestingSecurityContext,
                "my-catalog",
                requestedColumns);

        List<String> expectedRequests = requestedColumns
                .entrySet()
                .stream()
                .<String>mapMulti(
                        (requestedColumnsForTable, accepter) -> requestedColumnsForTable.getValue().forEach(
                                (col) -> accepter.accept("""
                                    {
                                        "operation": "FilterColumns",
                                        "resource": {
                                            "table": {
                                                "tableName": "%s",
                                                "schemaName": "my-schema",
                                                "catalogName": "my-catalog",
                                                "columns": ["%s"]
                                            }
                                        }
                                    }""".formatted(requestedColumnsForTable.getKey().getTableName(), col))))
                .collect(Collectors.toList());
        assertStringRequestsEqual(expectedRequests, this.mockClient.getRequests(), "/input/action");
        assertTrue(
                Maps.difference(
                        result,
                        Map.of(
                                tableOne, Set.of("table-one-column-one", "table-one-column-two"),
                                tableTwo, Set.of("table-two-column-two"),
                                tableThree, Set.of()))
                        .areEqual());
    }

    @Test
    public void testEmptyFilterColumns()
    {
        SchemaTableName someTable = SchemaTableName.schemaTableName("my-schema", "my-table");
        Map<SchemaTableName, Set<String>> requestedColumns = Map.of(someTable, Set.of());

        Map<SchemaTableName, Set<String>> result = authorizer.filterColumns(
                requestingSecurityContext,
                "my-catalog",
                requestedColumns);

        assertEquals(mockClient.getRequests().size(), 0);
        assertTrue(
                Maps.difference(
                        result,
                        requestedColumns).areEqual());
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
            BiFunction<OpaAccessControl, SystemSecurityContext, ?> callable,
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
        assertEquals(mockClient.getRequests().size(), 1);
    }
}
