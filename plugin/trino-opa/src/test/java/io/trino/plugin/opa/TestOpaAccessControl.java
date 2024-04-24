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
import io.trino.plugin.opa.AccessControlMethodHelpers.MethodWrapper;
import io.trino.plugin.opa.AccessControlMethodHelpers.ReturningMethodWrapper;
import io.trino.plugin.opa.AccessControlMethodHelpers.ThrowingMethodWrapper;
import io.trino.plugin.opa.HttpClientUtils.InstrumentedHttpClient;
import io.trino.plugin.opa.HttpClientUtils.MockResponse;
import io.trino.plugin.opa.TestConstants.TestingSystemAccessControlContext;
import io.trino.plugin.opa.schema.OpaViewExpression;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.SystemAccessControlFactory;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static io.trino.plugin.opa.RequestTestUtilities.assertStringRequestsEqual;
import static io.trino.plugin.opa.RequestTestUtilities.buildValidatingRequestHandler;
import static io.trino.plugin.opa.TestConstants.NO_ACCESS_RESPONSE;
import static io.trino.plugin.opa.TestConstants.OK_RESPONSE;
import static io.trino.plugin.opa.TestConstants.OPA_COLUMN_MASKING_URI;
import static io.trino.plugin.opa.TestConstants.OPA_ROW_FILTERING_URI;
import static io.trino.plugin.opa.TestConstants.OPA_SERVER_URI;
import static io.trino.plugin.opa.TestConstants.TEST_IDENTITY;
import static io.trino.plugin.opa.TestConstants.TEST_SECURITY_CONTEXT;
import static io.trino.plugin.opa.TestConstants.columnMaskingOpaConfig;
import static io.trino.plugin.opa.TestConstants.rowFilteringOpaConfig;
import static io.trino.plugin.opa.TestConstants.simpleOpaConfig;
import static io.trino.plugin.opa.TestHelpers.assertAccessControlMethodThrowsForIllegalResponses;
import static io.trino.plugin.opa.TestHelpers.assertAccessControlMethodThrowsForResponse;
import static io.trino.plugin.opa.TestHelpers.createMockHttpClient;
import static io.trino.plugin.opa.TestHelpers.createOpaAuthorizer;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOpaAccessControl
{
    @Test
    public void testResponseHasExtraFields()
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(TEST_IDENTITY, 200,"""
                {
                    "result": true,
                    "decision_id": "foo",
                    "some_debug_info": {"test": ""}
                }"""));
        OpaAccessControl authorizer = createOpaAuthorizer(simpleOpaConfig(), mockClient);
        authorizer.checkCanExecuteQuery(TEST_IDENTITY);
    }

    @Test
    public void testNoResourceAction()
    {
        testNoResourceAction("ExecuteQuery", OpaAccessControl::checkCanExecuteQuery);
        testNoResourceAction("ReadSystemInformation", OpaAccessControl::checkCanReadSystemInformation);
        testNoResourceAction("WriteSystemInformation", OpaAccessControl::checkCanWriteSystemInformation);
    }

    private void testNoResourceAction(String actionName, BiConsumer<OpaAccessControl, Identity> method)
    {
        Set<String> expectedRequests = ImmutableSet.of("""
                {
                    "operation": "%s"
                }""".formatted(actionName));
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(accessControl -> method.accept(accessControl, TEST_IDENTITY));
        assertAccessControlMethodBehaviour(wrappedMethod, expectedRequests);
    }

    @Test
    public void testTableResourceActions() {
        testTableResourceActions("ShowCreateTable", OpaAccessControl::checkCanShowCreateTable);
        testTableResourceActions("DropTable", OpaAccessControl::checkCanDropTable);
        testTableResourceActions("SetTableComment", OpaAccessControl::checkCanSetTableComment);
        testTableResourceActions("SetViewComment", OpaAccessControl::checkCanSetViewComment);
        testTableResourceActions("SetColumnComment", OpaAccessControl::checkCanSetColumnComment);
        testTableResourceActions("ShowColumns", OpaAccessControl::checkCanShowColumns);
        testTableResourceActions("AddColumn", OpaAccessControl::checkCanAddColumn);
        testTableResourceActions("DropColumn", OpaAccessControl::checkCanDropColumn);
        testTableResourceActions("AlterColumn", OpaAccessControl::checkCanAlterColumn);
        testTableResourceActions("RenameColumn", OpaAccessControl::checkCanRenameColumn);
        testTableResourceActions("InsertIntoTable", OpaAccessControl::checkCanInsertIntoTable);
        testTableResourceActions("DeleteFromTable", OpaAccessControl::checkCanDeleteFromTable);
        testTableResourceActions("TruncateTable", OpaAccessControl::checkCanTruncateTable);
        testTableResourceActions("CreateView", OpaAccessControl::checkCanCreateView);
        testTableResourceActions("DropView", OpaAccessControl::checkCanDropView);
        testTableResourceActions("RefreshMaterializedView", OpaAccessControl::checkCanRefreshMaterializedView);
        testTableResourceActions("DropMaterializedView", OpaAccessControl::checkCanDropMaterializedView);
    }

    private void testTableResourceActions(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName> callable)
    {
        CatalogSchemaTableName tableName = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(
                accessControl -> callable.accept(accessControl, TEST_SECURITY_CONTEXT, tableName));
        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "%s",
                            "schemaName": "%s",
                            "tableName": "%s"
                        }
                    }
                }
                """.formatted(
                        actionName,
                        tableName.getCatalogName(),
                        tableName.getSchemaTableName().getSchemaName(),
                        tableName.getSchemaTableName().getTableName());
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testTableWithPropertiesActions()
    {
        testTableWithPropertiesActions("SetTableProperties", OpaAccessControl::checkCanSetTableProperties);
        testTableWithPropertiesActions("SetMaterializedViewProperties", OpaAccessControl::checkCanSetMaterializedViewProperties);
        testTableWithPropertiesActions("CreateTable", OpaAccessControl::checkCanCreateTable);
        testTableWithPropertiesActions("CreateMaterializedView", OpaAccessControl::checkCanCreateMaterializedView);
    }

    private void testTableWithPropertiesActions(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Map> callable)
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        Map<String, Optional<Object>> properties = ImmutableMap.<String, Optional<Object>>builder()
                .put("string_item", Optional.of("string_value"))
                .put("empty_item", Optional.empty())
                .put("boxed_number_item", Optional.of(Integer.valueOf(32)))
                .buildOrThrow();
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(
                accessControl -> callable.accept(accessControl, TEST_SECURITY_CONTEXT, table, properties));
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
                                "boxed_number_item": 32
                            }
                        }
                    }
                }
                """.formatted(actionName);
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testIdentityResourceActions()
    {
        testIdentityResourceActions("ViewQueryOwnedBy", OpaAccessControl::checkCanViewQueryOwnedBy);
        testIdentityResourceActions("KillQueryOwnedBy", OpaAccessControl::checkCanKillQueryOwnedBy);
    }

    private void testIdentityResourceActions(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, Identity, Identity> callable)
    {
        Identity dummyIdentity = Identity.forUser("dummy-user")
                .withGroups(ImmutableSet.of("some-group"))
                .build();
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(
                accessControl -> callable.accept(accessControl, TEST_IDENTITY, dummyIdentity));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "user": {
                            "user": "dummy-user",
                            "groups": ["some-group"]
                        }
                    }
                }
                """.formatted(actionName);
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testStringResourceAction()
    {
        testStringResourceAction("SetSystemSessionProperty", "systemSessionProperty", (accessControl, systemSecurityContext, argument) -> accessControl.checkCanSetSystemSessionProperty(systemSecurityContext.getIdentity(), argument));
        testStringResourceAction("CreateCatalog", "catalog", OpaAccessControl::checkCanCreateCatalog);
        testStringResourceAction("DropCatalog", "catalog", OpaAccessControl::checkCanDropCatalog);
        testStringResourceAction("ShowSchemas", "catalog", OpaAccessControl::checkCanShowSchemas);
    }

    private void testStringResourceAction(
            String actionName,
            String resourceName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, String> callable)
    {
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(
                accessControl -> callable.accept(accessControl, TEST_SECURITY_CONTEXT, "resource_name"));
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
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testCanImpersonateUser()
    {
        String expectedRequest = """
                {
                    "operation": "ImpersonateUser",
                    "resource": {
                        "user": {
                            "user": "some_other_user"
                        }
                    }
                }
                """;
        assertAccessControlMethodBehaviour(
                new ThrowingMethodWrapper(accessControl -> accessControl.checkCanImpersonateUser(TEST_IDENTITY, "some_other_user")),
                ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testCanAccessCatalog()
    {
        ReturningMethodWrapper wrappedMethod = new ReturningMethodWrapper(
                accessControl -> accessControl.canAccessCatalog(TEST_SECURITY_CONTEXT, "test_catalog"));
        String expectedRequest = """
                {
                    "operation": "AccessCatalog",
                    "resource": {
                        "catalog": {
                            "name": "test_catalog"
                        }
                    }
                }""";
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testSchemaResourceActions()
    {
            testSchemaResourceActions("DropSchema", OpaAccessControl::checkCanDropSchema);
            testSchemaResourceActions("ShowCreateSchema", OpaAccessControl::checkCanShowCreateSchema);
            testSchemaResourceActions("ShowTables", OpaAccessControl::checkCanShowTables);
            testSchemaResourceActions("ShowFunctions", OpaAccessControl::checkCanShowFunctions);
    }

    private void testSchemaResourceActions(
            String actionName,
            FunctionalHelpers.Consumer3<OpaAccessControl, SystemSecurityContext, CatalogSchemaName> callable)
    {
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(
                accessControl -> callable.accept(accessControl, TEST_SECURITY_CONTEXT, new CatalogSchemaName("my_catalog", "my_schema")));

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
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testCreateSchema()
    {
        CatalogSchemaName schema = new CatalogSchemaName("my_catalog", "my_schema");
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(
                accessControl -> accessControl.checkCanCreateSchema(TEST_SECURITY_CONTEXT, schema, ImmutableMap.of()));
        String expectedRequest = """
                {
                    "operation": "CreateSchema",
                    "resource": {
                        "schema": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "properties": {}
                        }
                    }
                }""";
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testCreateSchemaWithProperties()
    {

        CatalogSchemaName schema = new CatalogSchemaName("my_catalog", "my_schema");
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(
                accessControl -> accessControl.checkCanCreateSchema(TEST_SECURITY_CONTEXT, schema, ImmutableMap.of("some_key", "some_value")));
        String expectedRequest = """
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
                }""";
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testRenameSchema()
    {
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(accessControl -> accessControl.checkCanRenameSchema(
                TEST_SECURITY_CONTEXT,
                new CatalogSchemaName("my_catalog", "my_schema"), "new_schema_name"));
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
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testRenameTableLikeObjects()
    {
        testRenameTableLikeObject("RenameTable", OpaAccessControl::checkCanRenameTable);
        testRenameTableLikeObject("RenameView", OpaAccessControl::checkCanRenameView);
        testRenameTableLikeObject("RenameMaterializedView", OpaAccessControl::checkCanRenameMaterializedView);
    }

    private void testRenameTableLikeObject(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, CatalogSchemaTableName> method)
    {
        CatalogSchemaTableName sourceTable = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        CatalogSchemaTableName targetTable = new CatalogSchemaTableName("my_catalog", "new_schema_name", "new_table_name");
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(
                accessControl -> method.accept(accessControl, TEST_SECURITY_CONTEXT, sourceTable, targetTable));

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
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testSetSchemaAuthorization()
    {
        CatalogSchemaName schema = new CatalogSchemaName("my_catalog", "my_schema");
        TrinoPrincipal principal = new TrinoPrincipal(PrincipalType.USER, "my_user");

        ThrowingMethodWrapper methodWrapper = new ThrowingMethodWrapper(
                accessControl -> accessControl.checkCanSetSchemaAuthorization(TEST_SECURITY_CONTEXT, schema, principal));

        String expectedRequest = """
                {
                    "operation": "SetSchemaAuthorization",
                    "resource": {
                        "schema": {
                            "catalogName": "%s",
                            "schemaName": "%s"
                        }
                    },
                    "grantee": {
                        "name": "%s",
                        "type": "%s"
                    }
                }
                """.formatted(schema.getCatalogName(), schema.getSchemaName(), principal.getName(), principal.getType());
        assertAccessControlMethodBehaviour(methodWrapper, ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testSetAuthorizationOnTableLikeObjects()
    {
        testSetAuthorizationOnTableLikeObject("SetTableAuthorization", OpaAccessControl::checkCanSetTableAuthorization);
        testSetAuthorizationOnTableLikeObject("SetViewAuthorization", OpaAccessControl::checkCanSetViewAuthorization);
    }

    private void testSetAuthorizationOnTableLikeObject(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, TrinoPrincipal> method)
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        TrinoPrincipal principal = new TrinoPrincipal(PrincipalType.USER, "my_user");
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(
                accessControl -> method.accept(accessControl, TEST_SECURITY_CONTEXT, table, principal));

        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "%s",
                            "schemaName": "%s",
                            "tableName": "%s"
                        }
                    },
                    "grantee": {
                        "name": "%s",
                        "type": "%s"
                    }
                }
                """.formatted(
                        actionName,
                        table.getCatalogName(),
                        table.getSchemaTableName().getSchemaName(),
                        table.getSchemaTableName().getTableName(),
                        principal.getName(),
                        principal.getType());
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testColumnOperationsOnTableLikeObjects()
    {
        testColumnOperationOnTableLikeObject("SelectFromColumns", OpaAccessControl::checkCanSelectFromColumns);
        testColumnOperationOnTableLikeObject("UpdateTableColumns", OpaAccessControl::checkCanUpdateTableColumns);
        testColumnOperationOnTableLikeObject("CreateViewWithSelectFromColumns", OpaAccessControl::checkCanCreateViewWithSelectFromColumns);
    }

    private void testColumnOperationOnTableLikeObject(
            String actionName,
            FunctionalHelpers.Consumer4<OpaAccessControl, SystemSecurityContext, CatalogSchemaTableName, Set<String>> method)
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        String dummyColumnName = "my_column";
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(
                accessControl -> method.accept(accessControl, TEST_SECURITY_CONTEXT, table, ImmutableSet.of(dummyColumnName)));
        String expectedRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "table": {
                            "catalogName": "%s",
                            "schemaName": "%s",
                            "tableName": "%s",
                            "columns": ["%s"]
                        }
                    }
                }
                """.formatted(
                        actionName,
                        table.getCatalogName(),
                        table.getSchemaTableName().getSchemaName(),
                        table.getSchemaTableName().getTableName(),
                        dummyColumnName);
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testCanSetCatalogSessionProperty()
    {
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(
                accessControl -> accessControl.checkCanSetCatalogSessionProperty(TEST_SECURITY_CONTEXT, "my_catalog", "my_property"));
        String expectedRequest = """
                {
                    "operation": "SetCatalogSessionProperty",
                    "resource": {
                        "catalogSessionProperty": {
                            "catalogName": "my_catalog",
                            "propertyName": "my_property"
                        }
                    }
                }
                """;
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testFunctionResourceActions()
    {
        CatalogSchemaRoutineName routine = new CatalogSchemaRoutineName("my_catalog", "my_schema", "my_routine_name");
        String baseRequest = """
                {
                    "operation": "%s",
                    "resource": {
                        "function": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "functionName": "my_routine_name"
                        }
                    }
                }""";
        assertAccessControlMethodBehaviour(
                new ThrowingMethodWrapper(authorizer -> authorizer.checkCanExecuteProcedure(TEST_SECURITY_CONTEXT, routine)),
                ImmutableSet.of(baseRequest.formatted("ExecuteProcedure")));
        assertAccessControlMethodBehaviour(
                new ThrowingMethodWrapper(authorizer -> authorizer.checkCanCreateFunction(TEST_SECURITY_CONTEXT, routine)),
                ImmutableSet.of(baseRequest.formatted("CreateFunction")));
        assertAccessControlMethodBehaviour(
                new ThrowingMethodWrapper(authorizer -> authorizer.checkCanDropFunction(TEST_SECURITY_CONTEXT, routine)),
                ImmutableSet.of(baseRequest.formatted("DropFunction")));
        assertAccessControlMethodBehaviour(
                new ReturningMethodWrapper(authorizer -> authorizer.canExecuteFunction(TEST_SECURITY_CONTEXT, routine)),
                ImmutableSet.of(baseRequest.formatted("ExecuteFunction")));
        assertAccessControlMethodBehaviour(
                new ReturningMethodWrapper(authorizer -> authorizer.canCreateViewWithExecuteFunction(TEST_SECURITY_CONTEXT, routine)),
                ImmutableSet.of(baseRequest.formatted("CreateViewWithExecuteFunction")));
    }

    @Test
    public void testCanExecuteTableProcedure()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        String expectedRequest = """
                {
                    "operation": "ExecuteTableProcedure",
                    "resource": {
                        "table": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "tableName": "my_table"
                        },
                        "function": {
                            "functionName": "my_procedure"
                        }
                    }
                }""";
        assertAccessControlMethodBehaviour(
                new ThrowingMethodWrapper(authorizer -> authorizer.checkCanExecuteTableProcedure(TEST_SECURITY_CONTEXT, table, "my_procedure")),
                ImmutableSet.of(expectedRequest));
    }

    @Test
    public void testRequestContextContentsWithKnownTrinoVersion()
    {
        testRequestContextContentsForGivenTrinoVersion(
                Optional.of(new TestingSystemAccessControlContext("12345.67890")),
                "12345.67890");
    }

    @Test
    public void testRequestContextContentsWithUnknownTrinoVersion()
    {
        testRequestContextContentsForGivenTrinoVersion(Optional.empty(), "UNKNOWN");
    }

    private void testRequestContextContentsForGivenTrinoVersion(Optional<SystemAccessControlFactory.SystemAccessControlContext> accessControlContext, String expectedTrinoVersion)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, request -> OK_RESPONSE);
        OpaAccessControl authorizer = (OpaAccessControl) OpaAccessControlFactory.create(
                ImmutableMap.of("opa.policy.uri", OPA_SERVER_URI.toString()),
                Optional.of(mockClient),
                accessControlContext);
        Identity sampleIdentityWithGroups = Identity.forUser("test_user").withGroups(ImmutableSet.of("some_group")).build();

        authorizer.checkCanExecuteQuery(sampleIdentityWithGroups);

        String expectedRequest = """
                {
                    "action": {
                        "operation": "ExecuteQuery"
                    },
                    "context": {
                        "identity": {
                            "user": "test_user",
                            "groups": ["some_group"]
                        },
                        "softwareStack": {
                            "trinoVersion": "%s"
                        }
                    }
                }""".formatted(expectedTrinoVersion);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input");
    }

    @Test
    public void testGetRowFiltersThrowsForIllegalResponse()
    {
        CatalogSchemaTableName tableName = new CatalogSchemaTableName("some_catalog", "some_schema", "some_table");
        Consumer<OpaAccessControl> methodUnderTest = authorizer -> authorizer.getRowFilters(TEST_SECURITY_CONTEXT, tableName);
        assertAccessControlMethodThrowsForIllegalResponses(methodUnderTest, rowFilteringOpaConfig(), OPA_ROW_FILTERING_URI);

        // Also test a valid JSON response, but containing invalid fields for a row filters request
        String validJsonButIllegalSchemaResponseContents = """
                {
                    "result": ["some-expr"]
                }""";
        MockResponse response = new MockResponse(validJsonButIllegalSchemaResponseContents, 200);

        assertAccessControlMethodThrowsForResponse(
                response,
                OPA_ROW_FILTERING_URI,
                rowFilteringOpaConfig(),
                methodUnderTest,
                OpaQueryException.class,
                "Failed to deserialize");
    }

    @Test
    public void testGetRowFilters()
    {
        // This example is a bit strange - an undefined policy would in most cases
        // result in an access denied situation. However, since this is row-level-filtering
        // we will accept this as meaning there are no known filters to be applied.
        testGetRowFilters("{}", ImmutableList.of());

        String noExpressionsResponse = """
                {
                    "result": []
                }""";
        testGetRowFilters(noExpressionsResponse, ImmutableList.of());

        String singleExpressionResponse = """
                {
                    "result": [
                        {"expression": "expr1"}
                    ]
                }""";
        testGetRowFilters(
                singleExpressionResponse,
                ImmutableList.of(new OpaViewExpression("expr1", Optional.empty())));

        String multipleExpressionsAndIdentitiesResponse = """
                {
                    "result": [
                        {"expression": "expr1"},
                        {"expression": "expr2", "identity": "expr2_identity"},
                        {"expression": "expr3", "identity": "expr3_identity"}
                    ]
                }""";
        testGetRowFilters(
                multipleExpressionsAndIdentitiesResponse,
                ImmutableList.<OpaViewExpression>builder()
                        .add(new OpaViewExpression("expr1", Optional.empty()))
                        .add(new OpaViewExpression("expr2", Optional.of("expr2_identity")))
                        .add(new OpaViewExpression("expr3", Optional.of("expr3_identity")))
                        .build());
    }

    private void testGetRowFilters(String responseContent, List<OpaViewExpression> expectedExpressions)
    {
        InstrumentedHttpClient httpClient = createMockHttpClient(OPA_ROW_FILTERING_URI, buildValidatingRequestHandler(TEST_IDENTITY, new MockResponse(responseContent, 200)));
        OpaAccessControl authorizer = createOpaAuthorizer(rowFilteringOpaConfig(), httpClient);
        CatalogSchemaTableName tableName = new CatalogSchemaTableName("some_catalog", "some_schema", "some_table");

        List<ViewExpression> result = authorizer.getRowFilters(TEST_SECURITY_CONTEXT, tableName);
        assertThat(result).allSatisfy(expression -> {
            assertThat(expression.getCatalog()).contains("some_catalog");
            assertThat(expression.getSchema()).contains("some_schema");
        });
        assertThat(result).map(
                viewExpression -> new OpaViewExpression(
                        viewExpression.getExpression(),
                        viewExpression.getSecurityIdentity()))
                .containsExactlyInAnyOrderElementsOf(expectedExpressions);

        String expectedRequest = """
                {
                    "operation": "GetRowFilters",
                    "resource": {
                        "table": {
                            "catalogName": "some_catalog",
                            "schemaName": "some_schema",
                            "tableName": "some_table"
                        }
                    }
                }""";
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), httpClient.getRequests(), "/input/action");
    }

    @Test
    public void testGetRowFiltersDoesNothingIfNotConfigured()
    {
        InstrumentedHttpClient httpClient = createMockHttpClient(
                OPA_SERVER_URI,
                request -> {
                    throw new AssertionError("Should not have been called");
                });
        OpaAccessControl authorizer = createOpaAuthorizer(simpleOpaConfig(), httpClient);
        CatalogSchemaTableName tableName = new CatalogSchemaTableName("some_catalog", "some_schema", "some_table");

        List<ViewExpression> result = authorizer.getRowFilters(TEST_SECURITY_CONTEXT, tableName);
        assertThat(result).isEmpty();
        assertThat(httpClient.getRequests()).isEmpty();
    }

    @Test
    public void testGetColumnMaskThrowsForIllegalResponse()
    {
        CatalogSchemaTableName tableName = new CatalogSchemaTableName("some_catalog", "some_schema", "some_table");
        Consumer<OpaAccessControl> methodUnderTest = authorizer -> authorizer.getColumnMask(TEST_SECURITY_CONTEXT, tableName, "some_column", VarcharType.VARCHAR);
        assertAccessControlMethodThrowsForIllegalResponses(methodUnderTest, columnMaskingOpaConfig(), OPA_COLUMN_MASKING_URI);

        // Also test a valid JSON response, but containing invalid fields for a row filters request
        String validJsonButIllegalSchemaResponseContents = """
                {
                    "result": {"expression": {"foo": "bar"}}
                }""";
        MockResponse response = new MockResponse(validJsonButIllegalSchemaResponseContents, 200);
        assertAccessControlMethodThrowsForResponse(
                response,
                OPA_COLUMN_MASKING_URI,
                columnMaskingOpaConfig(),
                methodUnderTest,
                OpaQueryException.class,
                "Failed to deserialize");
    }

    @Test
    public void testGetColumnMask()
    {
        // Similar note to the test for row level filtering:
        // This example is a bit strange - an undefined policy would in most cases
        // result in an access denied situation. However, since this is column masking,
        // we will accept this as meaning there are no masks to be applied.
        testGetColumnMask("{}", Optional.empty());

        String nullResponse = """
                {
                    "result": null
                }""";
        testGetColumnMask(nullResponse, Optional.empty());

        String expressionWithoutIdentityResponse = """
                {
                    "result": {"expression": "expr1"}
                }""";
        testGetColumnMask(
                expressionWithoutIdentityResponse,
                Optional.of(new OpaViewExpression("expr1", Optional.empty())));

        String expressionWithIdentityResponse = """
                {
                    "result": {"expression": "expr1", "identity": "some_identity"}
                }""";
        testGetColumnMask(
                expressionWithIdentityResponse,
                Optional.of(new OpaViewExpression("expr1", Optional.of("some_identity"))));
    }

    private void testGetColumnMask(String responseContent, Optional<OpaViewExpression> expectedExpression)
    {
        InstrumentedHttpClient httpClient = createMockHttpClient(
                OPA_COLUMN_MASKING_URI,
                buildValidatingRequestHandler(TEST_IDENTITY, new MockResponse(responseContent, 200)));
        OpaAccessControl authorizer = createOpaAuthorizer(columnMaskingOpaConfig(), httpClient);

        CatalogSchemaTableName tableName = new CatalogSchemaTableName("some_catalog", "some_schema", "some_table");

        Optional<ViewExpression> result = authorizer.getColumnMask(TEST_SECURITY_CONTEXT, tableName, "some_column", VarcharType.VARCHAR);

        assertThat(result.isEmpty()).isEqualTo(expectedExpression.isEmpty());
        assertThat(result.map(viewExpression -> {
            assertThat(viewExpression.getCatalog()).contains("some_catalog");
            assertThat(viewExpression.getSchema()).contains("some_schema");
            return new OpaViewExpression(viewExpression.getExpression(), viewExpression.getSecurityIdentity());
        })).isEqualTo(expectedExpression);

        String expectedRequest = """
                {
                    "operation": "GetColumnMask",
                    "resource": {
                        "column": {
                            "catalogName": "some_catalog",
                            "schemaName": "some_schema",
                            "tableName": "some_table",
                            "columnName": "some_column",
                            "columnType": "varchar"
                        }
                    }
                }""";
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), httpClient.getRequests(), "/input/action");
    }

    @Test
    public void testGetColumnMaskDoesNothingIfNotConfigured()
    {
        InstrumentedHttpClient httpClient = createMockHttpClient(
                OPA_SERVER_URI,
                request -> {
                    throw new AssertionError("Should not have been called");
                });
        OpaAccessControl authorizer = createOpaAuthorizer(simpleOpaConfig(), httpClient);
        CatalogSchemaTableName tableName = new CatalogSchemaTableName("some_catalog", "some_schema", "some_table");

        Optional<ViewExpression> result = authorizer.getColumnMask(TEST_SECURITY_CONTEXT, tableName, "some_column", VarcharType.VARCHAR);
        assertThat(result).isEmpty();
        assertThat(httpClient.getRequests()).isEmpty();
    }

    private static void assertAccessControlMethodBehaviour(MethodWrapper method, Set<String> expectedRequests)
    {
        InstrumentedHttpClient permissiveMockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(TEST_IDENTITY, OK_RESPONSE));
        InstrumentedHttpClient restrictiveMockClient = createMockHttpClient(OPA_SERVER_URI, buildValidatingRequestHandler(TEST_IDENTITY, NO_ACCESS_RESPONSE));

        assertThat(method.isAccessAllowed(createOpaAuthorizer(simpleOpaConfig(), permissiveMockClient))).isTrue();
        assertThat(method.isAccessAllowed(createOpaAuthorizer(simpleOpaConfig(), restrictiveMockClient))).isFalse();
        assertThat(permissiveMockClient.getRequests()).containsExactlyInAnyOrderElementsOf(restrictiveMockClient.getRequests());
        assertStringRequestsEqual(expectedRequests, permissiveMockClient.getRequests(), "/input/action");
        assertAccessControlMethodThrowsForIllegalResponses(method::isAccessAllowed, simpleOpaConfig(), OPA_SERVER_URI);
    }
}
