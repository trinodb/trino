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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.opa.RequestTestUtilities.buildValidatingRequestHandler;
import static io.trino.plugin.opa.RequestTestUtilities.toJsonNode;
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

final class TestOpaBatchAccessControlFiltering
{
    public static final String PATH_TO_FILTER_RESOURCES = "/input/action/filterResources";
    public static final OpaConfig OPA_CONFIG_NO_BATCH_SIZE = batchFilteringOpaConfig();
    public static final OpaConfig OPA_CONFIG_BATCH_SIZE_2 = batchFilteringOpaConfig().setOpaBatchSize(2);
    public static final OpaConfig OPA_CONFIG_BATCH_SIZE_3 = batchFilteringOpaConfig().setOpaBatchSize(3);
    public static final OpaConfig OPA_CONFIG_BATCH_SIZE_100 = batchFilteringOpaConfig().setOpaBatchSize(100);

    @Test
    void testFilterViewQueryOwnedBy()
    {
        List<Identity> inputIdentities = IntStream.rangeClosed(1, 11)
                .mapToObj(i -> Identity.ofUser("user-%s".formatted(i)))
                .collect(toImmutableList());

        Function<Identity, JsonNode> identityToJsonNode = identity ->
                toJsonNode("""
                        {
                            "user": {
                                "user" : "%s",
                                "groups": []
                            }
                        }
                        """.formatted(identity.getUser()));

        List<TestCase> testCases = ImmutableList.of(new TestCase(OPA_CONFIG_NO_BATCH_SIZE, ImmutableMap.of(11, 1), 1),
                new TestCase(OPA_CONFIG_BATCH_SIZE_2, ImmutableMap.of(2, 5, 1, 1), 6),
                new TestCase(OPA_CONFIG_BATCH_SIZE_3, ImmutableMap.of(3, 3, 2, 1), 4),
                new TestCase(OPA_CONFIG_BATCH_SIZE_100, ImmutableMap.of(11, 1), 1)
        );

        for (TestCase testCase : testCases) {
            assertAccessControlMethodBehaviour(
                    (accessControl, systemSecurityContext, identities) -> accessControl.filterViewQueryOwnedBy(systemSecurityContext.getIdentity(), identities),
                    inputIdentities,
                    identityToJsonNode,
                    new ExpectedRequestProperties(
                            testCase.batchSizeToBatchNumber,
                            testCase.numberOfRequest,
                            "FilterViewQueryOwnedBy",
                            inputIdentities.stream().map(identityToJsonNode).collect(toImmutableSet())),
                    testCase.opaConfig);
        }
    }

    @Test
    void testFilterCatalogs()
    {
        List<String> catalogs = IntStream.rangeClosed(1, 11)
                .mapToObj("catalog-%s"::formatted)
                .collect(toImmutableList());

        Function<String, JsonNode> catalogToJsonNode = e -> toJsonNode(
                """
                        {
                            "catalog":
                                {"name": "%s"}
                        }
                        """.formatted(e));
        List<TestCase> testCases = ImmutableList.of(new TestCase(OPA_CONFIG_NO_BATCH_SIZE, ImmutableMap.of(11, 1), 1),
                new TestCase(OPA_CONFIG_BATCH_SIZE_2, ImmutableMap.of(2, 5, 1, 1), 6),
                new TestCase(OPA_CONFIG_BATCH_SIZE_3, ImmutableMap.of(3, 3, 2, 1), 4),
                new TestCase(OPA_CONFIG_BATCH_SIZE_100, ImmutableMap.of(11, 1), 1)
        );

        for (TestCase testCase : testCases) {
            assertAccessControlMethodBehaviour(
                    OpaAccessControl::filterCatalogs,
                    catalogs,
                    catalogToJsonNode,
                    new ExpectedRequestProperties(testCase.batchSizeToBatchNumber,
                            testCase.numberOfRequest,
                            "FilterCatalogs",
                            catalogs.stream().map(catalogToJsonNode).collect(toImmutableSet())),
                    testCase.opaConfig);
        }
    }

    @Test
    void testFilterSchemas()
    {
        String catalog = "my_catalog";
        List<String> schemas = IntStream.rangeClosed(1, 11)
                .mapToObj("schema-%s"::formatted)
                .collect(toImmutableList());

        Function<String, JsonNode> schemaToJsonNode = schema -> toJsonNode("""
                {
                    "schema": {
                        "schemaName": "%s",
                        "catalogName": "%s"
                    }
                 }
                """.formatted(schema, catalog));

        List<TestCase> testCases = ImmutableList.of(new TestCase(OPA_CONFIG_NO_BATCH_SIZE, ImmutableMap.of(11, 1), 1),
                new TestCase(OPA_CONFIG_BATCH_SIZE_2, ImmutableMap.of(2, 5, 1, 1), 6),
                new TestCase(OPA_CONFIG_BATCH_SIZE_3, ImmutableMap.of(3, 3, 2, 1), 4),
                new TestCase(OPA_CONFIG_BATCH_SIZE_100, ImmutableMap.of(11, 1), 1)
        );

        for (TestCase testCase : testCases) {
            assertAccessControlMethodBehaviour(
                    (accessControl, systemSecurityContext, items) -> accessControl.filterSchemas(systemSecurityContext, catalog, items),
                    schemas,
                    schemaToJsonNode,
                    new ExpectedRequestProperties(testCase.batchSizeToBatchNumber,
                            testCase.numberOfRequest,
                            "FilterSchemas",
                            schemas.stream().map(schemaToJsonNode).collect(toImmutableSet())),
                    testCase.opaConfig);
        }
    }

    @Test
    void testFilterTables()
    {
        String catalogName = "my_catalog";
        List<String> schemas = IntStream.rangeClosed(1, 4)
                .mapToObj("schema-%s"::formatted)
                .collect(toImmutableList());

        List<SchemaTableName> tables = schemas.stream().map(s -> ImmutableList.of(
                new SchemaTableName(s, "table_one"),
                new SchemaTableName(s, "table_two"),
                new SchemaTableName(s, "table_three"))).flatMap(Collection::stream).collect(toImmutableList());

        Function<SchemaTableName, JsonNode> tableToJsonNode = table -> toJsonNode("""
                {
                    "table": {
                        "tableName": "%s",
                        "schemaName": "%s",
                        "catalogName": "%s"
                    }
                }
                """.formatted(table.getTableName(), table.getSchemaName(), catalogName));

        List<TestCase> testCases = ImmutableList.of(new TestCase(OPA_CONFIG_NO_BATCH_SIZE, ImmutableMap.of(12, 1), 1),
                new TestCase(OPA_CONFIG_BATCH_SIZE_2, ImmutableMap.of(2, 6), 6),
                new TestCase(OPA_CONFIG_BATCH_SIZE_3, ImmutableMap.of(3, 4), 4),
                new TestCase(OPA_CONFIG_BATCH_SIZE_100, ImmutableMap.of(12, 1), 1)
        );

        for (TestCase testCase : testCases) {
            assertAccessControlMethodBehaviour(
                    (accessControl, systemSecurityContext, items) -> accessControl.filterTables(systemSecurityContext, catalogName, items),
                    tables,
                    tableToJsonNode,
                    new ExpectedRequestProperties(testCase.batchSizeToBatchNumber,
                            testCase.numberOfRequest,
                            "FilterTables",
                            tables.stream().map(tableToJsonNode).collect(toImmutableSet())),
                    testCase.opaConfig);
        }
    }

    @Test
    void testFilterColumns()
    {
        String catalog = "my_catalog";
        String pathToResources = PATH_TO_FILTER_RESOURCES + "/0/table/columns";

        SchemaTableName tableOne = SchemaTableName.schemaTableName("my_schema", "table_one");
        SchemaTableName tableTwo = SchemaTableName.schemaTableName("my_schema", "table_two");
        SchemaTableName tableThree = SchemaTableName.schemaTableName("my_schema", "table_three");

        BiFunction<Integer, String, Set<String>> buildColumnSet = (numberOfColumns, tableName) -> IntStream.rangeClosed(1, numberOfColumns)
                .mapToObj(column -> "%s_column_%s".formatted(tableName, column))
                .collect(toImmutableSet());

        Map<SchemaTableName, Set<String>> requestedColumns = ImmutableMap.of(
                tableOne, buildColumnSet.apply(12, "table_one"),
                tableTwo, buildColumnSet.apply(10, "table_two"),
                tableThree, buildColumnSet.apply(7, "table_three"));

        List<TestCase> testCases = ImmutableList.of(new TestCase(OPA_CONFIG_NO_BATCH_SIZE, ImmutableMap.of(12, 1, 10, 1, 7, 1), 3),
                new TestCase(OPA_CONFIG_BATCH_SIZE_2, ImmutableMap.of(2, 6 + 5 + 3, 1, 1), 6 + 5 + 4),
                new TestCase(OPA_CONFIG_BATCH_SIZE_3, ImmutableMap.of(3, 4 + 3 + 2, 1, 2), 4 + 4 + 3),
                new TestCase(OPA_CONFIG_BATCH_SIZE_100, ImmutableMap.of(12, 1, 10, 1, 7, 1), 3)
        );

        for (TestCase testCase : testCases) {
            InstrumentedHttpClient mockClient = createMockHttpClient(
                    OPA_SERVER_BATCH_URI,
                    buildHandler(pathToResources,
                            ImmutableSet.of("\"table_one_column_1\"", "\"table_one_column_2\"", "\"table_one_column_3\"", "\"table_two_column_1\"")
                                    .stream()
                                    .map(RequestTestUtilities::toJsonNode)
                                    .collect(toImmutableSet())));

            OpaAccessControl authorizer = createOpaAuthorizer(testCase.opaConfig, mockClient);
            Map<SchemaTableName, Set<String>> result = authorizer.filterColumns(
                    TEST_SECURITY_CONTEXT,
                    catalog,
                    requestedColumns);

            assertThat(result).containsExactlyInAnyOrderEntriesOf(
                    ImmutableMap.of(tableOne, ImmutableSet.of("table_one_column_1", "table_one_column_2", "table_one_column_3"),
                            tableTwo, ImmutableSet.of("table_two_column_1")));

            assertRequestMatchesExpectedRequestProperties(new ExpectedRequestProperties(
                            testCase.batchSizeToBatchNumber,
                            testCase.numberOfRequest,
                            "FilterColumns",
                            requestedColumns
                                    .entrySet()
                                    .stream()
                                    .flatMap(entry -> entry.getValue().stream())
                                    .map(item -> toJsonNode("\"%s\"".formatted(item)))
                                    .collect(toImmutableSet())),
                    ImmutableList.copyOf(mockClient.getRequests()),
                    pathToResources);
        }
    }

    @Test
    void testEmptyFilterColumns()
    {
        String catalog = "my_catalog";

        assertFilteringAccessControlMethodDoesNotSendRequests(
                accessControl -> accessControl.filterColumns(TEST_SECURITY_CONTEXT, catalog, ImmutableMap.of()).entrySet());

        SchemaTableName tableOne = SchemaTableName.schemaTableName("my_schema", "table_one");
        SchemaTableName tableTwo = SchemaTableName.schemaTableName("my_schema", "table_two");
        Map<SchemaTableName, Set<String>> requestedColumns = ImmutableMap.of(
                tableOne, ImmutableSet.of()
                , tableTwo, ImmutableSet.of());

        assertFilteringAccessControlMethodDoesNotSendRequests(
                accessControl -> accessControl.filterColumns(TEST_SECURITY_CONTEXT, catalog, requestedColumns).entrySet());
    }

    @Test
    void testFilterColumnErrorCases()
    {
        assertAccessControlMethodThrowsForIllegalResponses(
                accessControl -> accessControl.filterColumns(
                        TEST_SECURITY_CONTEXT,
                        "my_catalog",
                        ImmutableMap.of(new SchemaTableName("some_schema", "some_table"),
                                ImmutableSet.of("column_one", "column_two"))),
                batchFilteringOpaConfig(),
                OPA_SERVER_BATCH_URI);
    }

    @Test
    void testFilterFunctions()
    {
        String catalog = "my_catalog";

        List<SchemaFunctionName> functions = IntStream
                .rangeClosed(1, 14)
                .mapToObj(i -> new SchemaFunctionName("my_schema", "function_%s".formatted(i)))
                .collect(toImmutableList());

        Function<SchemaFunctionName, JsonNode> schemaFunctionNameToJsonNode = function -> toJsonNode("""
                {
                    "function": {
                        "catalogName": "%s",
                        "schemaName": "%s",
                        "functionName": "%s"
                    }
                }
                """.formatted(catalog, function.getSchemaName(), function.getFunctionName()));

        List<TestCase> testCases = ImmutableList.of(new TestCase(OPA_CONFIG_NO_BATCH_SIZE, ImmutableMap.of(14, 1), 1),
                new TestCase(OPA_CONFIG_BATCH_SIZE_2, ImmutableMap.of(2, 7), 7),
                new TestCase(OPA_CONFIG_BATCH_SIZE_3, ImmutableMap.of(3, 4, 2, 1), 5),
                new TestCase(OPA_CONFIG_BATCH_SIZE_100, ImmutableMap.of(14, 1), 1)
        );

        for (TestCase testCase : testCases) {
            assertAccessControlMethodBehaviour(
                    (authorizer, systemSecurityContext, items) -> authorizer.filterFunctions(systemSecurityContext, catalog, items),
                    functions,
                    schemaFunctionNameToJsonNode,
                    new ExpectedRequestProperties(testCase.batchSizeToBatchNumber,
                            testCase.numberOfRequest,
                            "FilterFunctions",
                            functions.stream().map(schemaFunctionNameToJsonNode).collect(toImmutableSet())),
                    testCase.opaConfig);
        }
    }

    private static List<JsonNode> extractRequestElements(JsonNode request, String path)
    {
        ImmutableList.Builder<JsonNode> requestedItemsBuilder = ImmutableList.builder();
        request.at(path).elements().forEachRemaining(requestedItemsBuilder::add);
        return requestedItemsBuilder.build();
    }

    private static Function<JsonNode, MockResponse> buildHandler(String jsonPath, Set<JsonNode> resourcesToAccept)
    {
        return buildValidatingRequestHandler(TEST_IDENTITY, parsedRequest -> {
            List<JsonNode> requestedItems = extractRequestElements(parsedRequest, jsonPath);
            List<Integer> resultSet = IntStream.range(0, requestedItems.size())
                    .filter(i -> resourcesToAccept.contains(requestedItems.get(i))).boxed().collect(toImmutableList());
            String responseContent = "{\"result\": %s }".formatted(resultSet);
            return new MockResponse(responseContent, 200);
        });
    }

    record TestCase(OpaConfig opaConfig, Map<Integer, Integer> batchSizeToBatchNumber, Integer numberOfRequest) {}

    record ExpectedRequestProperties(Map<Integer, Integer> batchChunkSizes, int numberOfRequests, String operation, Set<JsonNode> expectedRequestItems) {}

    private static <T> void assertAccessControlMethodBehaviour(
            FunctionalHelpers.Function3<OpaAccessControl, SystemSecurityContext, Set<T>, Collection<T>> method,
            List<T> objectsToFilter, Function<T, JsonNode> valueExtractor, ExpectedRequestProperties expectedRequestProperties, OpaConfig config)
    {
        assertFilteringAccessControlMethodDoesNotSendRequests(accessControl -> method.apply(accessControl, TEST_SECURITY_CONTEXT, ImmutableSet.of()));

        List<Set<T>> allowedObjects = ImmutableList.of(
                ImmutableSet.of(),
                ImmutableSet.of(objectsToFilter.getFirst()),
                objectsToFilter.stream().collect(toImmutableSet()),
                objectsToFilter.stream().filter(obj -> objectsToFilter.indexOf(obj) % 2 == 0).collect(toImmutableSet()),
                objectsToFilter.stream().filter(obj -> objectsToFilter.indexOf(obj) % 2 != 0).collect(toImmutableSet()));

        record SubTestCase<T>(Set<T> expectedResourcesResult, Set<JsonNode> allowedResources)
        {

        }

        List<SubTestCase<T>> testCases = allowedObjects
                .stream()
                .map(items -> new SubTestCase<T>(items, items.stream().map(valueExtractor).collect(toImmutableSet())))
                .collect(toImmutableList());

        for (SubTestCase testCase : testCases) {
            InstrumentedHttpClient httpClient = createMockHttpClient(OPA_SERVER_BATCH_URI, buildHandler(PATH_TO_FILTER_RESOURCES, testCase.allowedResources));
            OpaAccessControl accessControl = createOpaAuthorizer(config, httpClient);
            var result = method.apply(accessControl, TEST_SECURITY_CONTEXT, objectsToFilter.stream().collect(toImmutableSet()));
            List<JsonNode> actualRequests = httpClient.getRequests();
            assertRequestMatchesExpectedRequestProperties(expectedRequestProperties, ImmutableList.copyOf(actualRequests), PATH_TO_FILTER_RESOURCES);
            assertThat(result).containsExactlyInAnyOrderElementsOf(testCase.expectedResourcesResult);
        }

        Consumer<OpaAccessControl> methodWithItems = accessControl -> method.apply(accessControl, TEST_SECURITY_CONTEXT, objectsToFilter.stream().collect(toImmutableSet()));
        assertAccessControlMethodThrowsForIllegalResponses(methodWithItems, config, OPA_SERVER_BATCH_URI);
        assertAccessControlMethodThrowsForResponse(
                new MockResponse("{\"result\": %s}".formatted(IntStream.rangeClosed(1, objectsToFilter.size() + 1).boxed().collect(toImmutableList())), 200),
                OPA_SERVER_BATCH_URI,
                config,
                methodWithItems,
                OpaQueryException.QueryFailed.class,
                "Failed to query OPA backend");
    }

    private static void assertRequestOperationMatches(ImmutableList<JsonNode> requests, String expectedOperation)
    {
        assertThat(requests.stream()
                .allMatch(request -> request.at("/input/action/operation").asText().equals(expectedOperation)))
                .isTrue();
    }

    private static void assertBatchSizesMatch(ImmutableList<JsonNode> requests, Map<Integer, Integer> expectedBatchSizes, String pathToResources)
    {
        Map<Integer, Integer> actualBatchSizes = new HashMap<>();
        requests.forEach(request -> {
            int batchSize = request.at(pathToResources).size();
            actualBatchSizes.put(batchSize, actualBatchSizes.getOrDefault(batchSize, 0) + 1);
        });
        assertThat(actualBatchSizes).isEqualTo(expectedBatchSizes);
    }

    private static void assertRequestedItemsMatch(ImmutableList<JsonNode> requests, ImmutableSet<JsonNode> expectedItems, String pathToResources)
    {
        Set<JsonNode> actualRequestedItems = requests.stream().map(request -> extractRequestElements(request, pathToResources)).flatMap(Collection::stream).collect(toImmutableSet());
        assertThat(actualRequestedItems).isEqualTo(expectedItems);
    }

    private static void assertRequestMatchesExpectedRequestProperties(ExpectedRequestProperties expectedRequestProperties, ImmutableList<JsonNode> requests, String pathToResources)
    {
        assertThat(requests.size()).isEqualTo(expectedRequestProperties.numberOfRequests);
        assertRequestOperationMatches(requests, expectedRequestProperties.operation);
        assertBatchSizesMatch(requests, expectedRequestProperties.batchChunkSizes, pathToResources);
        assertRequestedItemsMatch(requests, ImmutableSet.copyOf(expectedRequestProperties.expectedRequestItems), pathToResources);
    }

    private static void assertFilteringAccessControlMethodDoesNotSendRequests(Function<OpaAccessControl, Collection<?>> method)
    {
        InstrumentedHttpClient httpClientForEmptyRequest = createMockHttpClient(OPA_SERVER_BATCH_URI, request -> OK_RESPONSE);
        assertThat(method.apply(createOpaAuthorizer(OPA_CONFIG_BATCH_SIZE_2, httpClientForEmptyRequest))).isEmpty();
        assertThat(httpClientForEmptyRequest.getRequests()).isEmpty();
    }
}
