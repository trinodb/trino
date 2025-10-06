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
import static java.util.Objects.requireNonNull;
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

        Set<JsonNode> expectedIdentities = inputIdentities.stream().map(identityToJsonNode).collect(toImmutableSet());

        assertAccessControlMethodBehaviour(
                (accessControl, systemSecurityContext, identities) -> accessControl.filterViewQueryOwnedBy(systemSecurityContext.getIdentity(), identities),
                inputIdentities,
                identityToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(11, 1)),
                        1,
                        "FilterViewQueryOwnedBy",
                        expectedIdentities),
                OPA_CONFIG_NO_BATCH_SIZE);

        assertAccessControlMethodBehaviour(
                (accessControl, systemSecurityContext, identities) -> accessControl.filterViewQueryOwnedBy(systemSecurityContext.getIdentity(), identities),
                inputIdentities,
                identityToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(2, 5), new BatchSizeAndNumberOfRequests(1, 1)),
                        6,
                        "FilterViewQueryOwnedBy",
                        expectedIdentities),
                OPA_CONFIG_BATCH_SIZE_2);

        assertAccessControlMethodBehaviour(
                (accessControl, systemSecurityContext, identities) -> accessControl.filterViewQueryOwnedBy(systemSecurityContext.getIdentity(), identities),
                inputIdentities,
                identityToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(3, 3), new BatchSizeAndNumberOfRequests(2, 1)),
                        4,
                        "FilterViewQueryOwnedBy",
                        expectedIdentities),
                OPA_CONFIG_BATCH_SIZE_3);

        assertAccessControlMethodBehaviour(
                (accessControl, systemSecurityContext, identities) -> accessControl.filterViewQueryOwnedBy(systemSecurityContext.getIdentity(), identities),
                inputIdentities,
                identityToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(11, 1)),
                        1,
                        "FilterViewQueryOwnedBy",
                        expectedIdentities),
                OPA_CONFIG_BATCH_SIZE_100);
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

        Set<JsonNode> expectedRequestItems = catalogs.stream().map(catalogToJsonNode).collect(toImmutableSet());

        assertAccessControlMethodBehaviour(
                OpaAccessControl::filterCatalogs,
                catalogs,
                catalogToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(11, 1)),
                        1,
                        "FilterCatalogs",
                        expectedRequestItems),
                OPA_CONFIG_NO_BATCH_SIZE);

        assertAccessControlMethodBehaviour(
                OpaAccessControl::filterCatalogs,
                catalogs,
                catalogToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(2, 5), new BatchSizeAndNumberOfRequests(1, 1)),
                        6,
                        "FilterCatalogs",
                        expectedRequestItems),
                OPA_CONFIG_BATCH_SIZE_2);

        assertAccessControlMethodBehaviour(
                OpaAccessControl::filterCatalogs,
                catalogs,
                catalogToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(3, 3), new BatchSizeAndNumberOfRequests(2, 1)),
                        4,
                        "FilterCatalogs",
                        expectedRequestItems),
                OPA_CONFIG_BATCH_SIZE_3);

        assertAccessControlMethodBehaviour(
                OpaAccessControl::filterCatalogs,
                catalogs,
                catalogToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(11, 1)),
                        1,
                        "FilterCatalogs",
                        expectedRequestItems),
                OPA_CONFIG_BATCH_SIZE_100);
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

        Set<JsonNode> expectedRequestItems = schemas.stream().map(schemaToJsonNode).collect(toImmutableSet());

        assertAccessControlMethodBehaviour(
                (accessControl, systemSecurityContext, items) -> accessControl.filterSchemas(systemSecurityContext, catalog, items),
                schemas,
                schemaToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(11, 1)),
                        1,
                        "FilterSchemas",
                        expectedRequestItems),
                OPA_CONFIG_NO_BATCH_SIZE);

        assertAccessControlMethodBehaviour(
                (accessControl, systemSecurityContext, items) -> accessControl.filterSchemas(systemSecurityContext, catalog, items),
                schemas,
                schemaToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(2, 5), new BatchSizeAndNumberOfRequests(1, 1)),
                        6,
                        "FilterSchemas",
                        expectedRequestItems),
                OPA_CONFIG_BATCH_SIZE_2);

        assertAccessControlMethodBehaviour(
                (accessControl, systemSecurityContext, items) -> accessControl.filterSchemas(systemSecurityContext, catalog, items),
                schemas,
                schemaToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(3, 3), new BatchSizeAndNumberOfRequests(2, 1)),
                        4,
                        "FilterSchemas",
                        expectedRequestItems),
                OPA_CONFIG_BATCH_SIZE_3);

        assertAccessControlMethodBehaviour(
                (accessControl, systemSecurityContext, items) -> accessControl.filterSchemas(systemSecurityContext, catalog, items),
                schemas,
                schemaToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(11, 1)),
                        1,
                        "FilterSchemas",
                        expectedRequestItems),
                OPA_CONFIG_BATCH_SIZE_100);
    }

    @Test
    void testFilterTables()
    {
        String catalogName = "my_catalog";

        List<String> schemas = IntStream.rangeClosed(1, 4)
                .mapToObj("schema-%s"::formatted)
                .collect(toImmutableList());

        List<SchemaTableName> tables = schemas.stream()
                .map(s -> ImmutableList.of(
                        new SchemaTableName(s, "table_one"),
                        new SchemaTableName(s, "table_two"),
                        new SchemaTableName(s, "table_three")))
                .flatMap(Collection::stream)
                .collect(toImmutableList());

        Function<SchemaTableName, JsonNode> tableToJsonNode = table -> toJsonNode("""
                {
                    "table": {
                        "tableName": "%s",
                        "schemaName": "%s",
                        "catalogName": "%s"
                    }
                }
                """.formatted(table.getTableName(), table.getSchemaName(), catalogName));

        Set<JsonNode> expectedRequestItems = tables.stream().map(tableToJsonNode).collect(toImmutableSet());

        assertAccessControlMethodBehaviour(
                (accessControl, systemSecurityContext, items) -> accessControl.filterTables(systemSecurityContext, catalogName, items),
                tables,
                tableToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(12, 1)),
                        1,
                        "FilterTables",
                        expectedRequestItems),
                OPA_CONFIG_NO_BATCH_SIZE);

        assertAccessControlMethodBehaviour(
                (accessControl, systemSecurityContext, items) -> accessControl.filterTables(systemSecurityContext, catalogName, items),
                tables,
                tableToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(2, 6)),
                        6,
                        "FilterTables",
                        expectedRequestItems),
                OPA_CONFIG_BATCH_SIZE_2);

        assertAccessControlMethodBehaviour(
                (accessControl, systemSecurityContext, items) -> accessControl.filterTables(systemSecurityContext, catalogName, items),
                tables,
                tableToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(3, 4)),
                        4,
                        "FilterTables",
                        expectedRequestItems),
                OPA_CONFIG_BATCH_SIZE_3);

        assertAccessControlMethodBehaviour(
                (accessControl, systemSecurityContext, items) -> accessControl.filterTables(systemSecurityContext, catalogName, items),
                tables,
                tableToJsonNode,
                new ExpectedRequestProperties(
                        ImmutableSet.of(new BatchSizeAndNumberOfRequests(12, 1)),
                        1,
                        "FilterTables",
                        expectedRequestItems),
                OPA_CONFIG_BATCH_SIZE_100);
    }

    @Test
    void testFilterColumns()
    {
        String catalog = "my_catalog";

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

        Set<JsonNode> allowedResources = ImmutableSet.of("\"table_one_column_1\"", "\"table_one_column_2\"", "\"table_one_column_3\"", "\"table_two_column_1\"")
                .stream()
                .map(RequestTestUtilities::toJsonNode)
                .collect(toImmutableSet());

        Map<SchemaTableName, ImmutableSet<String>> expectedResult = ImmutableMap.of(tableOne, ImmutableSet.of("table_one_column_1", "table_one_column_2", "table_one_column_3"),
                tableTwo, ImmutableSet.of("table_two_column_1"));

        testFilterColummns(catalog, requestedColumns, allowedResources, expectedResult, OPA_CONFIG_NO_BATCH_SIZE,
                ImmutableSet.of(new BatchSizeAndNumberOfRequests(12, 1), new BatchSizeAndNumberOfRequests(10, 1), new BatchSizeAndNumberOfRequests(7, 1)),
                3);
        testFilterColummns(catalog, requestedColumns, allowedResources, expectedResult, OPA_CONFIG_BATCH_SIZE_2,
                ImmutableSet.of(new BatchSizeAndNumberOfRequests(2, 6 + 5 + 3), new BatchSizeAndNumberOfRequests(1, 1)),
                6 + 5 + 4);
        testFilterColummns(catalog, requestedColumns, allowedResources, expectedResult, OPA_CONFIG_BATCH_SIZE_3,
                ImmutableSet.of(new BatchSizeAndNumberOfRequests(3, 4 + 3 + 2), new BatchSizeAndNumberOfRequests(1, 2)), 4 + 4 + 3);
        testFilterColummns(catalog, requestedColumns, allowedResources, expectedResult, OPA_CONFIG_BATCH_SIZE_100,
                ImmutableSet.of(new BatchSizeAndNumberOfRequests(12, 1), new BatchSizeAndNumberOfRequests(10, 1), new BatchSizeAndNumberOfRequests(7, 1)),
                3);
    }

    private void testFilterColummns(String catalog, Map<SchemaTableName, Set<String>> requestedColumns, Set<JsonNode> allowedResources,
            Map<SchemaTableName, ImmutableSet<String>> expectedResult, OpaConfig opaConfig, ImmutableSet<BatchSizeAndNumberOfRequests> batchSizeToBatchNumber, int numberOfRequest)
    {

        String pathToResources = PATH_TO_FILTER_RESOURCES + "/0/table/columns";
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_BATCH_URI, buildHandler(pathToResources, allowedResources));

        OpaAccessControl authorizer = createOpaAuthorizer(opaConfig, mockClient);
        Map<SchemaTableName, Set<String>> result = authorizer.filterColumns(
                TEST_SECURITY_CONTEXT,
                catalog,
                requestedColumns);

        assertThat(result).containsExactlyInAnyOrderEntriesOf(expectedResult);

        assertRequestMatchesExpectedRequestProperties(new ExpectedRequestProperties(
                        batchSizeToBatchNumber,
                        numberOfRequest,
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

    @Test
    void testEmptyFilterColumns()
    {
        String catalog = "my_catalog";

        assertFilteringAccessControlMethodDoesNotSendRequests(
                accessControl -> accessControl.filterColumns(TEST_SECURITY_CONTEXT, catalog, ImmutableMap.of()).entrySet());

        SchemaTableName tableOne = SchemaTableName.schemaTableName("my_schema", "table_one");
        SchemaTableName tableTwo = SchemaTableName.schemaTableName("my_schema", "table_two");
        Map<SchemaTableName, Set<String>> requestedColumns = ImmutableMap.of(
                tableOne, ImmutableSet.of(),
                tableTwo, ImmutableSet.of());

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
                        ImmutableMap.of(new SchemaTableName("some_schema", "some_table"), ImmutableSet.of("column_one", "column_two"))),
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

        Set<JsonNode> expectedRequestItems = functions.stream().map(schemaFunctionNameToJsonNode).collect(toImmutableSet());

        assertAccessControlMethodBehaviour(
                (authorizer, systemSecurityContext, items) -> authorizer.filterFunctions(systemSecurityContext, catalog, items),
                functions,
                schemaFunctionNameToJsonNode,
                new ExpectedRequestProperties(ImmutableSet.of(new BatchSizeAndNumberOfRequests(14, 1)),
                        1,
                        "FilterFunctions",
                        expectedRequestItems),
                OPA_CONFIG_NO_BATCH_SIZE);

        assertAccessControlMethodBehaviour(
                (authorizer, systemSecurityContext, items) -> authorizer.filterFunctions(systemSecurityContext, catalog, items),
                functions,
                schemaFunctionNameToJsonNode,
                new ExpectedRequestProperties(ImmutableSet.of(new BatchSizeAndNumberOfRequests(2, 7)),
                        7,
                        "FilterFunctions",
                        expectedRequestItems),
                OPA_CONFIG_BATCH_SIZE_2);

        assertAccessControlMethodBehaviour(
                (authorizer, systemSecurityContext, items) -> authorizer.filterFunctions(systemSecurityContext, catalog, items),
                functions,
                schemaFunctionNameToJsonNode,
                new ExpectedRequestProperties(ImmutableSet.of(new BatchSizeAndNumberOfRequests(3, 4), new BatchSizeAndNumberOfRequests(2, 1)),
                        5,
                        "FilterFunctions",
                        expectedRequestItems),
                OPA_CONFIG_BATCH_SIZE_3);

        assertAccessControlMethodBehaviour(
                (authorizer, systemSecurityContext, items) -> authorizer.filterFunctions(systemSecurityContext, catalog, items),
                functions,
                schemaFunctionNameToJsonNode,
                new ExpectedRequestProperties(ImmutableSet.of(new BatchSizeAndNumberOfRequests(14, 1)),
                        1,
                        "FilterFunctions",
                        expectedRequestItems),
                OPA_CONFIG_BATCH_SIZE_100);
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

    record ExpectedRequestProperties(Set<BatchSizeAndNumberOfRequests> batchSizeAndNumberOfRequests, int numberOfRequests, String operation, Set<JsonNode> expectedRequestItems)
    {
        ExpectedRequestProperties
        {
            batchSizeAndNumberOfRequests = ImmutableSet.copyOf(requireNonNull(batchSizeAndNumberOfRequests, "batchSizeAndNumberOfRequests is null"));
            expectedRequestItems = ImmutableSet.copyOf(requireNonNull(expectedRequestItems, "expectedRequestItems is null"));
            requireNonNull(operation, "operation is null");
        }
    }

    record BatchSizeAndNumberOfRequests(int batchSize, int numberOfRequests)
    {

    }

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

        record TestCase<T>(Set<T> expectedResourcesResult, Set<JsonNode> allowedResources)
        {
            TestCase
            {
                expectedResourcesResult = ImmutableSet.copyOf(requireNonNull(expectedResourcesResult, "expectedResourcesResult is null"));
                allowedResources = ImmutableSet.copyOf(requireNonNull(allowedResources, "allowedResources is null"));
            }
        }

        List<TestCase<T>> testCases = allowedObjects
                .stream()
                .map(items -> new TestCase<T>(items, items.stream().map(valueExtractor).collect(toImmutableSet())))
                .collect(toImmutableList());

        for (TestCase<T> testCase : testCases) {
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

    private static void assertBatchSizesMatch(ImmutableList<JsonNode> requests, Set<BatchSizeAndNumberOfRequests> expectedBatchSizes, String pathToResources)
    {
        Map<Integer, Integer> actualBatchSizes = new HashMap<>();
        requests.forEach(request -> {
            int batchSize = request.at(pathToResources).size();
            actualBatchSizes.put(batchSize, actualBatchSizes.getOrDefault(batchSize, 0) + 1);
        });
        assertThat(actualBatchSizes.entrySet().stream().map(e -> new BatchSizeAndNumberOfRequests(e.getKey(), e.getValue())).collect(toImmutableSet()))
                .isEqualTo(expectedBatchSizes);
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
        assertBatchSizesMatch(requests, expectedRequestProperties.batchSizeAndNumberOfRequests, pathToResources);
        assertRequestedItemsMatch(requests, ImmutableSet.copyOf(expectedRequestProperties.expectedRequestItems), pathToResources);
    }

    private static void assertFilteringAccessControlMethodDoesNotSendRequests(Function<OpaAccessControl, Collection<?>> method)
    {
        InstrumentedHttpClient httpClientForEmptyRequest = createMockHttpClient(OPA_SERVER_BATCH_URI, request -> OK_RESPONSE);
        assertThat(method.apply(createOpaAuthorizer(OPA_CONFIG_BATCH_SIZE_2, httpClientForEmptyRequest))).isEmpty();
        assertThat(httpClientForEmptyRequest.getRequests()).isEmpty();
    }
}
