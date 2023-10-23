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
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
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

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.trino.plugin.opa.HttpClientUtils.InstrumentedHttpClient;
import static io.trino.plugin.opa.RequestTestUtilities.assertJsonRequestsEqual;
import static io.trino.plugin.opa.RequestTestUtilities.assertStringRequestsEqual;
import static io.trino.plugin.opa.TestHelpers.NO_ACCESS_RESPONSE;
import static io.trino.plugin.opa.TestHelpers.OK_RESPONSE;
import static io.trino.plugin.opa.TestHelpers.convertSystemSecurityContextToIdentityArgument;
import static io.trino.plugin.opa.TestHelpers.createFailingTestCases;
import static io.trino.plugin.opa.TestHelpers.createIllegalResponseTestCases;
import static io.trino.plugin.opa.TestHelpers.systemSecurityContextFromIdentity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#noResourceActionTestCases")
    public void testNoResourceAction(String actionName, BiConsumer<OpaAccessControl, SystemSecurityContext> method)
    {
        method.accept(authorizer, requestingSecurityContext);
        ObjectNode expectedRequest = jsonMapper.createObjectNode().put("operation", actionName);
        assertJsonRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#noResourceActionFailureTestCases")
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
        assertJsonRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
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
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#tableResourceTestCases")
    public void testTableResourceActions(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName> callable)
    {
        callable.accept(
                authorizer,
                requestingSecurityContext,
                new CatalogSchemaTableName("my_catalog", "my_schema", "my_table"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "tableName": "my_table"
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> tableResourceFailureTestCases()
    {
        return createFailingTestCases(tableResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#tableResourceFailureTestCases")
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
                        new CatalogSchemaTableName("my_catalog", "my_schema", "my_table")));
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
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#tableWithPropertiesTestCases")
    public void testTableWithPropertiesActions(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Map> callable)
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        Map<String, Optional<Object>> properties = new HashMap<>(
                ImmutableMap.<String, Optional<Object>>builder()
                        .put("string_item", Optional.of("string_value"))
                        .put("empty_item", Optional.empty())
                        .put("boxed_number_item", Optional.of(Integer.valueOf(32)))
                        .buildOrThrow());
        // https://openjdk.org/jeps/269
        // New collections do not support null items in them, so we need to ensure
        // our code can still deal with them.
        properties.put("null_item", null);

        callable.accept(authorizer, requestingSecurityContext, table, properties);

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "tableName": "my_table",
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "properties": {
                                "string_item": "string_value",
                                "empty_item": null,
                                "null_item": null,
                                "boxed_number_item": 32
                            }
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> tableWithPropertiesFailureTestCases()
    {
        return createFailingTestCases(tableWithPropertiesTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#tableWithPropertiesFailureTestCases")
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
                        new CatalogSchemaTableName("my_catalog", "my_schema", "my_table"),
                        ImmutableMap.of()));
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
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#identityResourceTestCases")
    public void testIdentityResourceActions(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, Identity, Identity> callable)
    {
        Identity dummyIdentity = Identity.forUser("dummy-user")
                .withGroups(ImmutableSet.of("some-group"))
                .withExtraCredentials(ImmutableMap.of("some_extra_credential", "value"))
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
                            "extraCredentials": {"some_extra_credential": "value"}
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> identityResourceFailureTestCases()
    {
        return createFailingTestCases(identityResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#identityResourceFailureTestCases")
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
                OpaAccessControl::checkCanCreateCatalog,
                OpaAccessControl::checkCanDropCatalog,
                OpaAccessControl::checkCanShowSchemas,
                OpaAccessControl::checkCanDropRole);
        Stream<FunctionalHelpers.Pair<String, String>> actionAndResource = Stream.of(
                FunctionalHelpers.Pair.of("ImpersonateUser", "user"),
                FunctionalHelpers.Pair.of("SetSystemSessionProperty", "systemSessionProperty"),
                FunctionalHelpers.Pair.of("CreateCatalog", "catalog"),
                FunctionalHelpers.Pair.of("DropCatalog", "catalog"),
                FunctionalHelpers.Pair.of("ShowSchemas", "catalog"),
                FunctionalHelpers.Pair.of("DropRole", "role"));
        return Streams.zip(
                actionAndResource,
                methods,
                (action, method) -> Arguments.of(Named.of(action.getFirst(), action.getFirst()), action.getSecond(), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#stringResourceTestCases")
    public void testStringResourceAction(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, String> callable)
    {
        callable.accept(authorizer, requestingSecurityContext, "resource_name");

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "%s": {
                            "name": "resource_name"
                        }
                    }
                }
                """.formatted(actionName, resourceName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    public static Stream<Arguments> stringResourceFailureTestCases()
    {
        return createFailingTestCases(stringResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#stringResourceFailureTestCases")
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
                        "dummy_value"));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanAccessCatalog()
    {
        mockClient.setHandler(request -> OK_RESPONSE);
        assertTrue(authorizer.canAccessCatalog(requestingSecurityContext, "my_catalog_one"));

        mockClient.setHandler(request -> NO_ACCESS_RESPONSE);
        assertFalse(authorizer.canAccessCatalog(requestingSecurityContext, "my_catalog_two"));

        Set<String> expectedRequests = ImmutableSet.of("my_catalog_one", "my_catalog_two").stream().map("""
                {
                    "operation": "AccessCatalog",
                    "resource": {
                        "catalog": {
                            "name": "%s"
                        }
                    }
                }
                """::formatted)
                .collect(toImmutableSet());
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.opa.TestHelpers#illegalResponseArgumentProvider")
    public void testCanAccessCatalogIllegalResponses(
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.canAccessCatalog(requestingSecurityContext, "my_catalog"));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> schemaResourceTestCases()
    {
        Stream<FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaName>> methods = Stream.of(
                OpaAccessControl::checkCanDropSchema,
                OpaAccessControl::checkCanShowCreateSchema,
                OpaAccessControl::checkCanShowTables,
                OpaAccessControl::checkCanShowFunctions);
        Stream<String> actions = Stream.of(
                "DropSchema",
                "ShowCreateSchema",
                "ShowTables",
                "ShowFunctions");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#schemaResourceTestCases")
    public void testSchemaResourceActions(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaName> callable)
    {
        callable.accept(authorizer, requestingSecurityContext, new CatalogSchemaName("my_catalog", "my_schema"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "schema": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema"
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    public static Stream<Arguments> schemaResourceFailureTestCases()
    {
        return createFailingTestCases(schemaResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#schemaResourceFailureTestCases")
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
                        new CatalogSchemaName("dummy_catalog", "dummy_schema")));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCreateSchema()
    {
        CatalogSchemaName schema = new CatalogSchemaName("my_catalog", "my_schema");
        authorizer.checkCanCreateSchema(requestingSecurityContext, schema, ImmutableMap.of("some_key", "some_value"));
        authorizer.checkCanCreateSchema(requestingSecurityContext, schema, ImmutableMap.of());

        List<String> expectedRequests = ImmutableList.<String>builder()
                .add("""
                    {
                        "operation": "CreateSchema",
                        "resource": {
                            "schema": {
                                "catalogName": "my_catalog",
                                "schemaName": "my_schema",
                                "properties": {
                                    "some_key": "some_value"
                                }
                            }
                        }
                    }
                    """)
                .add("""
                    {
                        "operation": "CreateSchema",
                        "resource": {
                            "schema": {
                                "catalogName": "my_catalog",
                                "schemaName": "my_schema",
                                "properties": {}
                            }
                        }
                    }
                    """)
                .build();
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestHelpers#allErrorCasesArgumentProvider")
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
                        new CatalogSchemaName("my_catalog", "my_schema"),
                        ImmutableMap.of()));
        assertTrue(actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanRenameSchema()
    {
        CatalogSchemaName sourceSchema = new CatalogSchemaName("my_catalog", "my_schema");
        authorizer.checkCanRenameSchema(requestingSecurityContext, sourceSchema, "new_schema_name");

        String expectedRequest = """
                {
                    "operation": "RenameSchema",
                    "resource": {
                        "schema": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema"
                        }
                    },
                    "targetResource": {
                        "schema": {
                            "catalogName": "my_catalog",
                            "schemaName": "new_schema_name"
                        }
                    }
                }
                """;
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestHelpers#allErrorCasesArgumentProvider")
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
                        new CatalogSchemaName("my_catalog", "my_schema"),
                        "new_schema_name"));
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
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#renameTableTestCases")
    public void testRenameTableActions(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, CatalogSchemaTableName> method)
    {
        CatalogSchemaTableName sourceTable = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        CatalogSchemaTableName targetTable = new CatalogSchemaTableName("my_catalog", "new_schema_name", "new_table_name");

        method.accept(authorizer, requestingSecurityContext, sourceTable, targetTable);

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "tableName": "my_table"
                        }
                    },
                    "targetResource": {
                        "table": {
                            "catalogName": "my_catalog",
                            "schemaName": "new_schema_name",
                            "tableName": "new_table_name"
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    public static Stream<Arguments> renameTableFailureTestCases()
    {
        return createFailingTestCases(renameTableTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#renameTableFailureTestCases")
    public void testRenameTableFailure(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, CatalogSchemaTableName> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        CatalogSchemaTableName sourceTable = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        CatalogSchemaTableName targetTable = new CatalogSchemaTableName("my_catalog", "new_schema_name", "new_table_name");
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
        CatalogSchemaName schema = new CatalogSchemaName("my_catalog", "my_schema");

        authorizer.checkCanSetSchemaAuthorization(requestingSecurityContext, schema, new TrinoPrincipal(PrincipalType.USER, "my_user"));

        String expectedRequest = """
                {
                    "operation": "SetSchemaAuthorization",
                    "resource": {
                        "schema": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema"
                        }
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "my_user",
                                "type": "USER"
                            }
                        ]
                    }
                }
                """;
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanSetSchemaAuthorizationFailure(
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        CatalogSchemaName schema = new CatalogSchemaName("my_catalog", "my_schema");
        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanSetSchemaAuthorization(
                        requestingSecurityContext,
                        schema,
                        new TrinoPrincipal(PrincipalType.USER, "my_user")));
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
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#setTableAuthorizationTestCases")
    public void testCanSetTableAuthorization(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, TrinoPrincipal> method)
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");

        method.accept(authorizer, requestingSecurityContext, table, new TrinoPrincipal(PrincipalType.USER, "my_user"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "tableName": "my_table"
                        }
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "my_user",
                                "type": "USER"
                            }
                        ]
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> setTableAuthorizationFailureTestCases()
    {
        return createFailingTestCases(setTableAuthorizationTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {3}")
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#setTableAuthorizationFailureTestCases")
    public void testCanSetTableAuthorizationFailure(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, TrinoPrincipal> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");

        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        table,
                        new TrinoPrincipal(PrincipalType.USER, "my_user")));
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
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#tableColumnOperationTestCases")
    public void testTableColumnOperations(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Set<String>> method)
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        Set<String> columns = ImmutableSet.of("my_column");

        method.accept(authorizer, requestingSecurityContext, table, columns);

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "tableName": "my_table",
                            "columns": ["my_column"]
                        }
                    }
                }
                """.formatted(actionName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> tableColumnOperationFailureTestCases()
    {
        return createFailingTestCases(tableColumnOperationTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#tableColumnOperationFailureTestCases")
    public void testTableColumnOperationsFailure(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Set<String>> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);
        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        Set<String> columns = ImmutableSet.of("my_column");

        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(authorizer, requestingSecurityContext, table, columns));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanSetCatalogSessionProperty()
    {
        authorizer.checkCanSetCatalogSessionProperty(
                requestingSecurityContext, "my_catalog", "my_property");

        String expectedRequest = """
                {
                    "operation": "SetCatalogSessionProperty",
                    "resource": {
                        "catalog": {
                            "name": "my_catalog"
                        },
                        "catalogSessionProperty": {
                            "name": "my_property"
                        }
                    }
                }
                """;
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestHelpers#allErrorCasesArgumentProvider")
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
                        "my_catalog",
                        "my_property"));
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
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#schemaPrivilegeTestCases")
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
                new CatalogSchemaName("my_catalog", "my_schema"),
                new TrinoPrincipal(PrincipalType.USER, "my_user"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "schema": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema"
                        }
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "my_user",
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
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#schemaPrivilegeFailureTestCases")
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
                        new CatalogSchemaName("my_catalog", "my_schema"),
                        new TrinoPrincipal(PrincipalType.USER, "my_user")));
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
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#tablePrivilegeTestCases")
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
                new CatalogSchemaTableName("my_catalog", "my_schema", "my_table"),
                new TrinoPrincipal(PrincipalType.USER, "my_user"));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "tableName": "my_table"
                        }
                    },
                    "grantee": {
                        "principals": [
                            {
                                "name": "my_user",
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
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#tablePrivilegeFailureTestCases")
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
                        new CatalogSchemaTableName("my_catalog", "my_schema", "my_table"),
                        new TrinoPrincipal(PrincipalType.USER, "my_user")));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanCreateRole()
    {
        authorizer.checkCanCreateRole(requestingSecurityContext, "my_role_without_grantor", Optional.empty());
        TrinoPrincipal grantor = new TrinoPrincipal(PrincipalType.USER, "my_grantor");
        authorizer.checkCanCreateRole(requestingSecurityContext, "my_role_with_grantor", Optional.of(grantor));

        Set<String> expectedRequests = ImmutableSet.<String>builder()
                .add("""
                    {
                        "operation": "CreateRole",
                        "resource": {
                            "role": {
                                "name": "my_role_without_grantor"
                            }
                        }
                    }
                """)
                .add("""
                    {
                        "operation": "CreateRole",
                        "resource": {
                            "role": {
                                "name": "my_role_with_grantor"
                            }
                        },
                        "grantor": {
                            "name": "my_grantor",
                            "type": "USER"
                        }
                    }
                    """)
                .build();
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestHelpers#allErrorCasesArgumentProvider")
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
                        "my_role_without_grantor",
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
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#roleGrantingTestCases")
    public void testRoleGranting(
            String actionName,
            FunctionalHelpers.Consumer6<OpaAccessControl, SystemSecurityContext, Set<String>, Set<TrinoPrincipal>, Boolean, Optional<TrinoPrincipal>> method)
    {
        TrinoPrincipal grantee = new TrinoPrincipal(PrincipalType.ROLE, "my_grantee_role");
        method.accept(authorizer, requestingSecurityContext, ImmutableSet.of("my_role_without_grantor"), ImmutableSet.of(grantee), true, Optional.empty());

        TrinoPrincipal grantor = new TrinoPrincipal(PrincipalType.USER, "my_grantor_user");
        method.accept(authorizer, requestingSecurityContext, ImmutableSet.of("my_role_with_grantor"), ImmutableSet.of(grantee), false, Optional.of(grantor));

        Set<String> expectedRequests = ImmutableSet.<String>builder()
                .add("""
                    {
                        "operation": "%s",
                        "resource": {
                            "roles": [
                                {
                                    "name": "my_role_with_grantor"
                                }
                            ]
                        },
                        "grantor": {
                            "name": "my_grantor_user",
                            "type": "USER"
                        },
                        "grantee": {
                            "principals": [
                                {
                                    "name": "my_grantee_role",
                                    "type": "ROLE"
                                }
                            ],
                            "grantOption": false
                        }
                    }
                    """.formatted(actionName))
                .add("""
                    {
                        "operation": "%s",
                        "resource": {
                            "roles": [
                                {
                                    "name": "my_role_without_grantor"
                                }
                            ]
                        },
                        "grantee": {
                            "principals": [
                                {
                                    "name": "my_grantee_role",
                                    "type": "ROLE"
                                }
                            ],
                            "grantOption": true
                        }
                    }
                    """.formatted(actionName))
                .build();
        assertStringRequestsEqual(expectedRequests, mockClient.getRequests(), "/input/action");
    }

    private static Stream<Arguments> roleGrantingFailureTestCases()
    {
        return createFailingTestCases(roleGrantingTestCases());
    }

    @ParameterizedTest(name = "{index}: {0} - {2}")
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#roleGrantingFailureTestCases")
    public void testRoleGrantingFailure(
            String actionName,
            FunctionalHelpers.Consumer6<OpaAccessControl, SystemSecurityContext, Set<String>, Set<TrinoPrincipal>, Boolean, Optional<TrinoPrincipal>> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        TrinoPrincipal grantee = new TrinoPrincipal(PrincipalType.ROLE, "my_grantee_role");
        Throwable actualError = assertThrows(
                expectedException,
                () -> method.accept(
                        authorizer,
                        requestingSecurityContext,
                        ImmutableSet.of("my_role_without_grantor"),
                        ImmutableSet.of(grantee),
                        true,
                        Optional.empty()));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> functionResourceTestCases()
    {
        Stream<TestHelpers.MethodWrapper<CatalogSchemaRoutineName>> methods = Stream.of(
                new TestHelpers.ThrowingMethodWrapper<>(OpaAccessControl::checkCanExecuteProcedure),
                new TestHelpers.ReturningMethodWrapper<>(OpaAccessControl::canExecuteFunction),
                new TestHelpers.ReturningMethodWrapper<>(OpaAccessControl::canCreateViewWithExecuteFunction));
        Stream<String> actions = Stream.of(
                "ExecuteProcedure",
                "ExecuteFunction",
                "CreateViewWithExecuteFunction");
        return Streams.zip(actions, methods, (action, method) -> Arguments.of(Named.of(action, action), method));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#functionResourceTestCases")
    public void testFunctionResourceAction(
            String actionName,
            TestHelpers.MethodWrapper<CatalogSchemaRoutineName> method)
    {
        CatalogSchemaRoutineName routine = new CatalogSchemaRoutineName("my_catalog", "my_schema", "my_routine_name");
        assertTrue(method.isAccessAllowed(authorizer, requestingSecurityContext, routine));

        mockClient.setHandler(request -> NO_ACCESS_RESPONSE);
        assertFalse(method.isAccessAllowed(authorizer, requestingSecurityContext, routine));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "function": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "functionName": "my_routine_name"
                        }
                    }
                }""".formatted(actionName);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
        assertEquals(mockClient.getRequests().size(), 2);
    }

    private static Stream<Arguments> functionResourceIllegalResponseTestCases()
    {
        return createIllegalResponseTestCases(functionResourceTestCases());
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.OpaAccessControlUnitTest#functionResourceIllegalResponseTestCases")
    public void testFunctionResourceIllegalResponses(
            String actionName,
            TestHelpers.MethodWrapper<CatalogSchemaRoutineName> method,
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        CatalogSchemaRoutineName routine = new CatalogSchemaRoutineName("my_catalog", "my_schema", "my_routine_name");
        Throwable actualError = assertThrows(
                expectedException,
                () -> method.isAccessAllowed(authorizer, requestingSecurityContext, routine));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    @Test
    public void testCanExecuteTableProcedure()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        authorizer.checkCanExecuteTableProcedure(requestingSecurityContext, table, "my_procedure");

        String expectedRequest = """
                {
                    "operation": "ExecuteTableProcedure",
                    "resource": {
                        "table": {
                            "schemaName": "my_schema",
                            "catalogName": "my_catalog",
                            "tableName": "my_table"
                        },
                        "function": {
                            "functionName": "my_procedure"
                        }
                    }
                }""";
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input/action");
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("io.trino.plugin.opa.TestHelpers#allErrorCasesArgumentProvider")
    public void testCanExecuteTableProcedureFailure(
            HttpClientUtils.MockResponse failureResponse,
            Class<? extends Throwable> expectedException,
            String expectedErrorMessage)
    {
        mockClient.setHandler(request -> failureResponse);

        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        Throwable actualError = assertThrows(
                expectedException,
                () -> authorizer.checkCanExecuteTableProcedure(
                        requestingSecurityContext,
                        table,
                        "my_procedure"));
        assertTrue(
                actualError.getMessage().contains(expectedErrorMessage),
                String.format("Error must contain '%s': %s", expectedErrorMessage, actualError.getMessage()));
    }

    private static Stream<Arguments> noResourceActionTestCases()
    {
        Stream<BiConsumer<OpaAccessControl, SystemSecurityContext>> methods = Stream.of(
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
}
