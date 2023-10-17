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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Streams;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.function.FunctionKind;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.trino.plugin.openpolicyagent.HttpClientUtils.InstrumentedHttpClient;
import static io.trino.plugin.openpolicyagent.RequestTestUtilities.assertJsonRequestsEqual;
import static io.trino.plugin.openpolicyagent.RequestTestUtilities.assertStringRequestsEqual;
import static io.trino.plugin.openpolicyagent.TestHelpers.OK_RESPONSE;
import static io.trino.plugin.openpolicyagent.TestHelpers.convertSystemSecurityContextToIdentityArgument;
import static io.trino.plugin.openpolicyagent.TestHelpers.createFailingTestCases;
import static io.trino.plugin.openpolicyagent.TestHelpers.systemSecurityContextFromIdentity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OpaAccessControlUnitTest
{
    private static final URI OPA_SERVER_URI = URI.create("http://my-uri/");
    private InstrumentedHttpClient mockClient;
    private OpaAccessControl authorizer;
    private final JsonMapper jsonMapper = new JsonMapper();
    private Identity requestingIdentity;
    private SystemSecurityContext requestingSecurityContext;

    @BeforeEach
    public void setupAuthorizer()
    {
        this.mockClient = new InstrumentedHttpClient(OPA_SERVER_URI, "POST", JSON_UTF_8.toString(), request -> OK_RESPONSE);
        this.authorizer = (OpaAccessControl) new OpaAccessControlFactory().create(
                Map.of("opa.policy.uri", OPA_SERVER_URI.toString()),
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

    private static Stream<Arguments> noResourceActionTestCases()
    {
        Stream<BiConsumer<OpaAccessControl, SystemSecurityContext>> methods =
                Stream.of(
                        convertSystemSecurityContextToIdentityArgument(OpaAccessControl::checkCanExecuteQuery),
                        convertSystemSecurityContextToIdentityArgument(OpaAccessControl::checkCanReadSystemInformation),
                        convertSystemSecurityContextToIdentityArgument(OpaAccessControl::checkCanWriteSystemInformation),
                        OpaAccessControl::checkCanShowRoles,
                        OpaAccessControl::checkCanShowCurrentRoles,
                        OpaAccessControl::checkCanShowRoleGrants);
        Stream<String> expectedActions = Stream.of(
                "ExecuteQuery",
                "ReadSystemInformation",
                "WriteSystemInformation",
                "ShowRoles",
                "ShowCurrentRoles",
                "ShowRoleGrants");
        return Streams.zip(expectedActions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    private static Stream<Arguments> noResourceActionFailureTestCases()
    {
        return createFailingTestCases(noResourceActionTestCases());
    }

    @Test
    public void testResponseHasExtraFields()
    {
        mockClient.setHandler(request -> new HttpClientUtils.MockResponse("""
                {
                    "result": true,
                    "decision_id": "foo",
                    "some_debug_info": {"test": ""}
                }
                """,
                200));
        authorizer.checkCanShowRoles(requestingSecurityContext);
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#noResourceActionTestCases")
    public void testNoResourceAction(String actionName, BiConsumer<OpaAccessControl, SystemSecurityContext> method)
    {
        method.accept(authorizer, requestingSecurityContext);
        ObjectNode expectedRequest = jsonMapper.createObjectNode().put("operation", actionName);
        assertJsonRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#noResourceActionFailureTestCases")
    public void testNoResourceActionFailure(
            String actionName,
            BiConsumer<OpaAccessControl, SystemSecurityContext> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(authorizer, requestingSecurityContext));
        ObjectNode expectedRequest = jsonMapper.createObjectNode().put("operation", actionName);
        assertJsonRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> tableResourceTestCases()
    {
        Stream<FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName>> methods = Stream.of(
                OpaAccessControl::checkCanShowCreateTable,
                OpaAccessControl::checkCanDropTable,
                OpaAccessControl::checkCanSetTableComment,
                OpaAccessControl::checkCanSetViewComment,
                OpaAccessControl::checkCanSetColumnComment,
                OpaAccessControl::checkCanShowColumns,
                OpaAccessControl::checkCanAddColumn,
                OpaAccessControl::checkCanDropColumn,
                OpaAccessControl::checkCanAlterColumn,
                OpaAccessControl::checkCanRenameColumn,
                OpaAccessControl::checkCanInsertIntoTable,
                OpaAccessControl::checkCanDeleteFromTable,
                OpaAccessControl::checkCanTruncateTable,
                OpaAccessControl::checkCanCreateView,
                OpaAccessControl::checkCanDropView,
                OpaAccessControl::checkCanRefreshMaterializedView,
                OpaAccessControl::checkCanDropMaterializedView);
        Stream<String> actions = Stream.of(
                "ShowCreateTable",
                "DropTable",
                "SetTableComment",
                "SetViewComment",
                "SetColumnComment",
                "ShowColumns",
                "AddColumn",
                "DropColumn",
                "AlterColumn",
                "RenameColumn",
                "InsertIntoTable",
                "DeleteFromTable",
                "TruncateTable",
                "CreateView",
                "DropView",
                "RefreshMaterializedView",
                "DropMaterializedView");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#tableResourceTestCases")
    public void testTableResourceActions(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName> callable)
    {
        callable.accept(
                authorizer,
                requestingSecurityContext,
                new CatalogSchemaTableName("my-catalog", "my-schema", "my-table"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "my-catalog",
                            "schemaName": "my-schema",
                            "tableName": "my-table"
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> tableResourceFailureTestCases()
    {
        return createFailingTestCases(tableResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#tableResourceFailureTestCases")
    public void testTableResourceFailure(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        new CatalogSchemaTableName("catalog", "schema", "table")));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> tableWithPropertiesTestCases()
    {
        Stream<FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Map>> methods = Stream.of(
                OpaAccessControl::checkCanSetTableProperties,
                OpaAccessControl::checkCanSetMaterializedViewProperties,
                OpaAccessControl::checkCanCreateTable,
                OpaAccessControl::checkCanCreateMaterializedView);
        Stream<String> actions = Stream.of(
                "SetTableProperties",
                "SetMaterializedViewProperties",
                "CreateTable",
                "CreateMaterializedView");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#tableWithPropertiesTestCases")
    public void testTableWithPropertiesActions(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Map> callable)
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("my-catalog", "my-schema", "my-table");
        Map<String, Optional<Object>> properties = new HashMap<>();
        properties.put("string-item", Optional.of("string-value"));
        // https://openjdk.org/jeps/269
        // New collections do not support null items in them, so we need to ensure
        // our code will use a Map that can deal with nulls
        properties.put("empty-item", Optional.empty());
        properties.put("null-item", null);
        properties.put("boxed-number-item", Optional.of(Integer.valueOf(32)));

        callable.accept(authorizer, requestingSecurityContext, table, properties);

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "tableName": "my-table",
                            "catalogName": "my-catalog",
                            "schemaName": "my-schema",
                            "properties": {
                                "string-item": "string-value",
                                "empty-item": null,
                                "null-item": null,
                                "boxed-number-item": 32
                            }
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> tableWithPropertiesFailureTestCases()
    {
        return createFailingTestCases(tableWithPropertiesTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#tableWithPropertiesFailureTestCases")
    public void testTableWithPropertiesActionFailure(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Map> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        new CatalogSchemaTableName("catalog", "schema", "table"),
                        Map.of()));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> identityResourceTestCases()
    {
        Stream<FunctionalHelpers.Consumer3<OpaAccessControl, Identity, Identity>> methods = Stream.of(
                OpaAccessControl::checkCanViewQueryOwnedBy,
                OpaAccessControl::checkCanKillQueryOwnedBy);
        Stream<String> actions = Stream.of(
                "ViewQueryOwnedBy",
                "KillQueryOwnedBy");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#identityResourceTestCases")
    public void testIdentityResourceActions(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, Identity, Identity> callable)
    {
        Identity dummyIdentity = Identity.forUser("dummy-user")
                .withGroups(Set.of("some-group"))
                .withExtraCredentials(Map.of("some-extra-credential", "value"))
                .build();
        callable.accept(authorizer, requestingIdentity, dummyIdentity);

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "user": {
                            "name": "dummy-user",
                            "user": "dummy-user",
                            "groups": ["some-group"],
                            "enabledRoles": [],
                            "catalogRoles": {},
                            "extraCredentials": {"some-extra-credential": "value"}
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> identityResourceFailureTestCases()
    {
        return createFailingTestCases(identityResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#identityResourceFailureTestCases")
    public void testIdentityResourceActionsFailure(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, Identity, Identity> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(
                        authorizer,
                        requestingIdentity,
                        Identity.ofUser("dummy-user")));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> stringResourceTestCases()
    {
        Stream<FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, String>> methods = Stream.of(
                convertSystemSecurityContextToIdentityArgument(OpaAccessControl::checkCanImpersonateUser),
                convertSystemSecurityContextToIdentityArgument(OpaAccessControl::checkCanSetSystemSessionProperty),
                OpaAccessControl::checkCanAccessCatalog,
                OpaAccessControl::checkCanCreateCatalog,
                OpaAccessControl::checkCanDropCatalog,
                OpaAccessControl::checkCanShowSchemas,
                OpaAccessControl::checkCanDropRole,
                OpaAccessControl::checkCanExecuteFunction);
        Stream<FunctionalHelpers.Pair<String, String>> actionAndResource = Stream.of(
                FunctionalHelpers.Pair.of("ImpersonateUser", "user"),
                FunctionalHelpers.Pair.of("SetSystemSessionProperty", "systemSessionProperty"),
                FunctionalHelpers.Pair.of("AccessCatalog", "catalog"),
                FunctionalHelpers.Pair.of("CreateCatalog", "catalog"),
                FunctionalHelpers.Pair.of("DropCatalog", "catalog"),
                FunctionalHelpers.Pair.of("ShowSchemas", "catalog"),
                FunctionalHelpers.Pair.of("DropRole", "role"),
                FunctionalHelpers.Pair.of("ExecuteFunction", "function"));
        return Streams.zip(
                actionAndResource,
                methods,
                (action, method) -> Arguments.of(Named.of(action.getFirst(), action.getFirst()), action.getSecond(), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#stringResourceTestCases")
    public void testStringResourceAction(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, String> callable)
    {
        callable.accept(authorizer, requestingSecurityContext, "dummy-name");

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "%s": {
                            "name": "dummy-name"
                        }
                    }
                }
                """.formatted(actionName, resourceName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    public static Stream<Arguments> stringResourceFailureTestCases()
    {
        return createFailingTestCases(stringResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#stringResourceFailureTestCases")
    public void testStringResourceActionsFailure(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, String> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        "dummy-value"));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> schemaResourceTestCases()
    {
        Stream<FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaName>> methods = Stream.of(
                OpaAccessControl::checkCanDropSchema,
                OpaAccessControl::checkCanShowCreateSchema,
                OpaAccessControl::checkCanShowTables);
        Stream<String> actions = Stream.of(
                "DropSchema",
                "ShowCreateSchema",
                "ShowTables");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#schemaResourceTestCases")
    public void testSchemaResourceActions(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaName> callable)
    {
        callable.accept(authorizer, requestingSecurityContext, new CatalogSchemaName("my-catalog", "my-schema"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "schema": {
                            "catalogName": "my-catalog",
                            "schemaName": "my-schema"
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    public static Stream<Arguments> schemaResourceFailureTestCases()
    {
        return createFailingTestCases(schemaResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#schemaResourceFailureTestCases")
    public void testSchemaResourceActionsFailure(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaName> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        new CatalogSchemaName("dummy-catalog", "dummy-schema")));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCreateSchema()
    {
        CatalogSchemaName schema = new CatalogSchemaName("some-catalog", "some-schema");
        authorizer.checkCanCreateSchema(requestingSecurityContext, schema, Map.of("some-key", "some-value"));
        authorizer.checkCanCreateSchema(requestingSecurityContext, schema, Map.of());

        List<String> expectedRequests = List.of(
                """
                        {
                            "operation": "CreateSchema",
                            "resource": {
                                "schema": {
                                    "catalogName": "some-catalog",
                                    "schemaName": "some-schema",
                                    "properties": {
                                        "some-key": "some-value"
                                    }
                                }
                            }
                        }
                        """,
                """
                        {
                            "operation": "CreateSchema",
                            "resource": {
                                "schema": {
                                    "catalogName": "some-catalog",
                                    "schemaName": "some-schema",
                                    "properties": {}
                                }
                            }
                        }
                        """);
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCreateSchemaFailure(
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanCreateSchema(
                        requestingSecurityContext,
                        new CatalogSchemaName("some-catalog", "some-schema"),
                        Map.of()));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanRenameSchema()
    {
        CatalogSchemaName sourceSchema = new CatalogSchemaName("some-catalog", "some-schema");
        authorizer.checkCanRenameSchema(requestingSecurityContext, sourceSchema, "new-name");

        String expectedRequest = """
                {
                    "operation": "RenameSchema",
                    "resource": {
                        "schema": {
                            "catalogName": "some-catalog",
                            "schemaName": "some-schema"
                        }
                    },
                    "targetResource": {
                        "schema": {
                            "catalogName": "some-catalog",
                            "schemaName": "new-name"
                        }
                    }
                }
                """;
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanRenameSchemaFailure(
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanRenameSchema(
                        requestingSecurityContext,
                        new CatalogSchemaName("some-catalog", "some-schema"),
                        "new-name"));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> renameTableTestCases()
    {
        Stream<FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, CatalogSchemaTableName>> methods = Stream.of(
                OpaAccessControl::checkCanRenameTable,
                OpaAccessControl::checkCanRenameView,
                OpaAccessControl::checkCanRenameMaterializedView);
        Stream<String> actions = Stream.of(
                "RenameTable",
                "RenameView",
                "RenameMaterializedView");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#renameTableTestCases")
    public void testRenameTableActions(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, CatalogSchemaTableName> method)
    {
        CatalogSchemaTableName sourceTable = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");
        CatalogSchemaTableName targetTable = new CatalogSchemaTableName("another-catalog", "another-schema", "another-table");

        method.accept(authorizer, requestingSecurityContext, sourceTable, targetTable);

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "some-catalog",
                            "schemaName": "some-schema",
                            "tableName": "some-table"
                        }
                    },
                    "targetResource": {
                        "table": {
                            "catalogName": "another-catalog",
                            "schemaName": "another-schema",
                            "tableName": "another-table"
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    public static Stream<Arguments> renameTableFailureTestCases()
    {
        return createFailingTestCases(renameTableTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#renameTableFailureTestCases")
    public void testRenameTableFailure(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, CatalogSchemaTableName> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        CatalogSchemaTableName sourceTable = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");
        CatalogSchemaTableName targetTable = new CatalogSchemaTableName("another-catalog", "another-schema", "another-table");
        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        sourceTable,
                        targetTable));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanSetSchemaAuthorization()
    {
        CatalogSchemaName schema = new CatalogSchemaName("some-catalog", "some-schema");

        authorizer.checkCanSetSchemaAuthorization(requestingSecurityContext, schema, new TrinoPrincipal(PrincipalType.USER, "some-user"));

        String expectedRequest = """
                {
                    "operation": "SetSchemaAuthorization",
                    "resource": {
                        "schema": {
                            "catalogName": "some-catalog",
                            "schemaName": "some-schema"
                        }
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "some-user",
                                "type": "USER"
                            }
                        ]
                    }
                }
                """;
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanSetSchemaAuthorizationFailure(
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        CatalogSchemaName schema = new CatalogSchemaName("some-catalog", "some-schema");
        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanSetSchemaAuthorization(
                        requestingSecurityContext,
                        schema,
                        new TrinoPrincipal(PrincipalType.USER, "some-user")));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> setTableAuthorizationTestCases()
    {
        Stream<FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, TrinoPrincipal>> methods = Stream.of(
                OpaAccessControl::checkCanSetTableAuthorization,
                OpaAccessControl::checkCanSetViewAuthorization);
        Stream<String> actions = Stream.of(
                "SetTableAuthorization",
                "SetViewAuthorization");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#setTableAuthorizationTestCases")
    public void testCanSetTableAuthorization(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, TrinoPrincipal> method)
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");

        method.accept(authorizer, requestingSecurityContext, table, new TrinoPrincipal(PrincipalType.USER, "some-user"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "some-catalog",
                            "schemaName": "some-schema",
                            "tableName": "some-table"
                        }
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "some-user",
                                "type": "USER"
                            }
                        ]
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> setTableAuthorizationFailureTestCases()
    {
        return createFailingTestCases(setTableAuthorizationTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#setTableAuthorizationFailureTestCases")
    public void testCanSetTableAuthorizationFailure(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, TrinoPrincipal> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        CatalogSchemaTableName table = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");

        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        table,
                        new TrinoPrincipal(PrincipalType.USER, "some-user")));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> tableColumnOperationTestCases()
    {
        Stream<FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Set<String>>> methods = Stream.of(
                OpaAccessControl::checkCanSelectFromColumns,
                OpaAccessControl::checkCanUpdateTableColumns,
                OpaAccessControl::checkCanCreateViewWithSelectFromColumns);
        Stream<String> actionAndResource = Stream.of(
                "SelectFromColumns",
                "UpdateTableColumns",
                "CreateViewWithSelectFromColumns");
        return Streams.zip(actionAndResource, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#tableColumnOperationTestCases")
    public void testTableColumnOperations(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Set<String>> method)
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");
        Set<String> columns = Set.of("some-column");

        method.accept(authorizer, requestingSecurityContext, table, columns);

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "some-catalog",
                            "schemaName": "some-schema",
                            "tableName": "some-table",
                            "columns": ["some-column"]
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> tableColumnOperationFailureTestCases()
    {
        return createFailingTestCases(tableColumnOperationTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#tableColumnOperationFailureTestCases")
    public void testTableColumnOperationsFailure(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Set<String>> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);
        CatalogSchemaTableName table = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");
        Set<String> columns = Set.of("some-column");

        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(authorizer, requestingSecurityContext, table, columns));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanGrantExecuteFunctionPrivilege()
    {
        authorizer.checkCanGrantExecuteFunctionPrivilege(
                requestingSecurityContext,
                "some-function",
                new TrinoPrincipal(PrincipalType.USER, "some-user"),
                true);

        String expectedRequest = """
                {
                    "operation": "GrantExecuteFunctionPrivilege",
                    "resource": {
                        "function": {
                            "name": "some-function"
                        }
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "some-user",
                                "type": "USER"
                            }
                        ],
                        "grantOption": true
                    }
                }
                """;
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanGrantExecuteFunctionPrivilegeFailure(
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanGrantExecuteFunctionPrivilege(
                        requestingSecurityContext,
                        "some-function",
                        new TrinoPrincipal(PrincipalType.USER, "some-name"),
                        true));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanGrantExecuteFunctionPrivilegeWithFunctionKind()
    {
        authorizer.checkCanGrantExecuteFunctionPrivilege(
                requestingSecurityContext,
                FunctionKind.AGGREGATE,
                new CatalogSchemaRoutineName("some-catalog", "some-schema", "some-routine"),
                new TrinoPrincipal(PrincipalType.USER, "some-user"),
                true);

        String expectedRequest = """
                {
                    "operation": "GrantExecuteFunctionPrivilege",
                    "resource": {
                        "function": {
                            "name": "some-routine",
                            "functionKind": "AGGREGATE"
                        },
                        "schema": {
                            "catalogName": "some-catalog",
                            "schemaName": "some-schema"
                        }
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "some-user",
                                "type": "USER"
                            }
                        ],
                        "grantOption": true
                    }
                }
                """;
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanGrantExecuteFunctionPrivilegeWithFunctionKindFailure(
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanGrantExecuteFunctionPrivilege(
                        requestingSecurityContext,
                        FunctionKind.AGGREGATE,
                        new CatalogSchemaRoutineName("some-catalog", "some-schema", "some-routine"),
                        new TrinoPrincipal(PrincipalType.USER, "some-name"),
                        true));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanSetCatalogSessionProperty()
    {
        authorizer.checkCanSetCatalogSessionProperty(
                requestingSecurityContext, "some-catalog", "some-property");

        String expectedRequest = """
                {
                    "operation": "SetCatalogSessionProperty",
                    "resource": {
                        "catalog": {
                            "name": "some-catalog"
                        },
                        "catalogSessionProperty": {
                            "name": "some-property"
                        }
                    }
                }
                """;
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanSetCatalogSessionPropertyFailure(
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanSetCatalogSessionProperty(
                        requestingSecurityContext,
                        "some-catalog",
                        "some-property"));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> schemaPrivilegeTestCases()
    {
        Stream<FunctionalHelpers.Consumer5<OpaAccessControl, SystemSecurityContext, Privilege, CatalogSchemaName, TrinoPrincipal>> methods = Stream.of(
                OpaAccessControl::checkCanDenySchemaPrivilege,
                (authorizer, context, privilege, catalog, principal) -> authorizer.checkCanGrantSchemaPrivilege(
                        context, privilege, catalog, principal, true),
                (authorizer, context, privilege, catalog, principal) -> authorizer.checkCanRevokeSchemaPrivilege(
                        context, privilege, catalog, principal, true));
        Stream<String> actions = Stream.of(
                "DenySchemaPrivilege",
                "GrantSchemaPrivilege",
                "RevokeSchemaPrivilege");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#schemaPrivilegeTestCases")
    public void testSchemaPrivileges(
            String actionName,
            FunctionalHelpers.Consumer5<OpaAccessControl, SystemSecurityContext, Privilege, CatalogSchemaName, TrinoPrincipal> method)
            throws IOException
    {
        Privilege privilege = Privilege.CREATE;
        method.accept(
                authorizer,
                requestingSecurityContext,
                privilege,
                new CatalogSchemaName("some-catalog", "some-schema"),
                new TrinoPrincipal(PrincipalType.USER, "some-user"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "schema": {
                            "catalogName": "some-catalog",
                            "schemaName": "some-schema"
                        }
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "some-user",
                                "type": "USER"
                            }
                        ],
                        "privilege": "CREATE",
                        "grantOption": true
                    }
                }
                """.formatted(actionName);
        List<String> actualRequests = mockClient.getRequests();
        assertEquals(actualRequests.size(), 1, "Unexpected number of requests");

        JsonNode actualRequestInput = jsonMapper.readTree(mockClient.getRequests().get(0)).at("/input/action");
        if (!actualRequestInput.at("/grantee").has("grantOption")) {
            // The DenySchemaPrivilege request does not have a grant option, we'll default it to true so we can use the same test
            ((ObjectNode) actualRequestInput.at("/grantee")).put("grantOption", true);
        }
        assertEquals(jsonMapper.readTree(expectedRequest), actualRequestInput);
    }

    private static Stream<Arguments> schemaPrivilegeFailureTestCases()
    {
        return createFailingTestCases(schemaPrivilegeTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#schemaPrivilegeFailureTestCases")
    public void testSchemaPrivilegesFailure(
            String actionName,
            FunctionalHelpers.Consumer5<OpaAccessControl, SystemSecurityContext, Privilege, CatalogSchemaName, TrinoPrincipal> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Privilege privilege = Privilege.CREATE;
        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        privilege,
                        new CatalogSchemaName("some-catalog", "some-schema"),
                        new TrinoPrincipal(PrincipalType.USER, "some-user")));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> tablePrivilegeTestCases()
    {
        Stream<FunctionalHelpers.Consumer5<OpaAccessControl, SystemSecurityContext, Privilege, CatalogSchemaTableName, TrinoPrincipal>> methods = Stream.of(
                OpaAccessControl::checkCanDenyTablePrivilege,
                (authorizer, context, privilege, catalog, principal) -> authorizer.checkCanGrantTablePrivilege(context, privilege, catalog, principal, true),
                (authorizer, context, privilege, catalog, principal) -> authorizer.checkCanRevokeTablePrivilege(context, privilege, catalog, principal, true));
        Stream<String> actions = Stream.of(
                "DenyTablePrivilege",
                "GrantTablePrivilege",
                "RevokeTablePrivilege");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#tablePrivilegeTestCases")
    public void testTablePrivileges(
            String actionName,
            FunctionalHelpers.Consumer5<OpaAccessControl, SystemSecurityContext, Privilege, CatalogSchemaTableName, TrinoPrincipal> method)
            throws IOException
    {
        Privilege privilege = Privilege.CREATE;
        method.accept(
                authorizer,
                requestingSecurityContext,
                privilege,
                new CatalogSchemaTableName("some-catalog", "some-schema", "some-table"),
                new TrinoPrincipal(PrincipalType.USER, "some-user"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "some-catalog",
                            "schemaName": "some-schema",
                            "tableName": "some-table"
                        }
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "some-user",
                                "type": "USER"
                            }
                        ],
                        "privilege": "CREATE",
                        "grantOption": true
                    }
                }
                """.formatted(actionName);
        List<String> actualRequests = mockClient.getRequests();
        assertEquals(actualRequests.size(), 1, "Unexpected number of requests");

        JsonNode actualRequestInput = jsonMapper.readTree(mockClient.getRequests().get(0)).at("/input/action");
        if (!actualRequestInput.at("/grantee").has("grantOption")) {
            // The DenySchemaPrivilege request does not have a grant option, we'll default it to true so we can use the same test
            ((ObjectNode) actualRequestInput.at("/grantee")).put("grantOption", true);
        }
        assertEquals(jsonMapper.readTree(expectedRequest), actualRequestInput);
    }

    private static Stream<Arguments> tablePrivilegeFailureTestCases()
    {
        return createFailingTestCases(tablePrivilegeTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#tablePrivilegeFailureTestCases")
    public void testTablePrivilegesFailure(
            String actionName,
            FunctionalHelpers.Consumer5<OpaAccessControl, SystemSecurityContext, Privilege, CatalogSchemaTableName, TrinoPrincipal> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Privilege privilege = Privilege.CREATE;
        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        privilege,
                        new CatalogSchemaTableName("some-catalog", "some-schema", "some-table"),
                        new TrinoPrincipal(PrincipalType.USER, "some-user")));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanCreateRole()
    {
        authorizer.checkCanCreateRole(requestingSecurityContext, "some-role-without-grantor", Optional.empty());
        TrinoPrincipal grantor = new TrinoPrincipal(PrincipalType.USER, "some-grantor");
        authorizer.checkCanCreateRole(requestingSecurityContext, "some-role-with-grantor", Optional.of(grantor));

        Set<String> expectedRequests = Set.of("""
                {
                    "operation": "CreateRole",
                    "resource": {
                        "role": {
                            "name": "some-role-without-grantor"
                        }
                    }
                }
                """,
                """
                {
                    "operation": "CreateRole",
                    "resource": {
                        "role": {
                            "name": "some-role-with-grantor"
                        }
                    },
                    "grantor": {
                        "name": "some-grantor",
                        "type": "USER"
                    }
                }
                """);
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanCreateRoleFailure(
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanCreateRole(
                        requestingSecurityContext,
                        "some-role-without-grantor",
                        Optional.empty()));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> roleGrantingTestCases()
    {
        Stream<FunctionalHelpers.Consumer6<OpaAccessControl, SystemSecurityContext, Set<String>, Set<TrinoPrincipal>, Boolean, Optional<TrinoPrincipal>>> methods = Stream.of(
                OpaAccessControl::checkCanGrantRoles,
                OpaAccessControl::checkCanRevokeRoles);
        Stream<String> actions = Stream.of(
                "GrantRoles",
                "RevokeRoles");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#roleGrantingTestCases")
    public void testRoleGranting(
            String actionName,
            FunctionalHelpers.Consumer6<OpaAccessControl, SystemSecurityContext, Set<String>, Set<TrinoPrincipal>, Boolean, Optional<TrinoPrincipal>> method)
    {
        TrinoPrincipal grantee = new TrinoPrincipal(PrincipalType.ROLE, "some-grantee-role");
        method.accept(authorizer, requestingSecurityContext, Set.of("some-role-without-grantor"), Set.of(grantee), true, Optional.empty());

        TrinoPrincipal grantor = new TrinoPrincipal(PrincipalType.USER, "some-grantor-user");
        method.accept(authorizer, requestingSecurityContext, Set.of("some-role-with-grantor"), Set.of(grantee), false, Optional.of(grantor));

        Set<String> expectedRequests = Set.of("""
                {
                    "operation": "%s",
                    "resource": {
                        "roles": [
                            {
                                "name": "some-role-with-grantor"
                            }
                        ]
                    },
                    "grantor": {
                        "name": "some-grantor-user",
                        "type": "USER"
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "some-grantee-role",
                                "type": "ROLE"
                            }
                        ],
                        "grantOption": false
                    }
                }
                """.formatted(actionName),
                """
                {
                    "operation": "%s",
                    "resource": {
                        "roles": [
                            {
                                "name": "some-role-without-grantor"
                            }
                        ]
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "some-grantee-role",
                                "type": "ROLE"
                            }
                        ],
                        "grantOption": true
                    }
                }
                """.formatted(actionName));
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> roleGrantingFailureTestCases()
    {
        return createFailingTestCases(roleGrantingTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.openpolicyagent.OpaAccessControlUnitTest#roleGrantingFailureTestCases")
    public void testRoleGrantingFailure(
            String actionName,
            FunctionalHelpers.Consumer6<OpaAccessControl, SystemSecurityContext, Set<String>, Set<TrinoPrincipal>, Boolean, Optional<TrinoPrincipal>> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        TrinoPrincipal grantee = new TrinoPrincipal(PrincipalType.ROLE, "some-grantee-role");
        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        Set.of("some-role-without-grantor"),
                        Set.of(grantee),
                        true,
                        Optional.empty()));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanExecuteProcedure()
    {
        CatalogSchemaRoutineName routine = new CatalogSchemaRoutineName("some-catalog", "some-schema", "some-routine-name");
        authorizer.checkCanExecuteProcedure(requestingSecurityContext, routine);

        String expectedRequest = """
                {
                    "operation": "ExecuteProcedure",
                    "resource": {
                        "schema": {
                            "schemaName": "some-schema",
                            "catalogName": "some-catalog"
                        },
                        "function": {
                            "name": "some-routine-name"
                        }
                    }
                }""";
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanExecuteProcedureFailure(
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        CatalogSchemaRoutineName routine = new CatalogSchemaRoutineName("some-catalog", "some-schema", "some-routine-name");
        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanExecuteProcedure(
                        requestingSecurityContext,
                        routine));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanExecuteTableProcedure()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");
        authorizer.checkCanExecuteTableProcedure(requestingSecurityContext, table, "some-procedure");

        String expectedRequest = """
                {
                    "operation": "ExecuteTableProcedure",
                    "resource": {
                        "table": {
                            "schemaName": "some-schema",
                            "catalogName": "some-catalog",
                            "tableName": "some-table"
                        },
                        "function": {
                            "name": "some-procedure"
                        }
                    }
                }""";
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanExecuteTableProcedureFailure(
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        CatalogSchemaTableName table = new CatalogSchemaTableName("some-catalog", "some-schema", "some-table");
        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanExecuteTableProcedure(
                        requestingSecurityContext,
                        table,
                        "some-procedure"));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanExecuteFunctionWithFunctionKind()
    {
        CatalogSchemaRoutineName routine = new CatalogSchemaRoutineName("some-catalog", "some-schema", "some-routine");
        authorizer.checkCanExecuteFunction(requestingSecurityContext, FunctionKind.AGGREGATE, routine);

        String expectedRequest = """
                {
                    "operation": "ExecuteFunction",
                    "resource": {
                        "schema": {
                            "schemaName": "some-schema",
                            "catalogName": "some-catalog"
                        },
                        "function": {
                            "name": "some-routine",
                            "functionKind": "AGGREGATE"
                        }
                    }
                }""";
        assertStringRequestsEqual(Set.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.openpolicyagent.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanExecuteFunctionWithFunctionKindFailure(
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        CatalogSchemaRoutineName routine = new CatalogSchemaRoutineName("some-catalog", "some-schema", "some-routine");
        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanExecuteFunction(
                        requestingSecurityContext,
                        FunctionKind.AGGREGATE,
                        routine));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }
}
