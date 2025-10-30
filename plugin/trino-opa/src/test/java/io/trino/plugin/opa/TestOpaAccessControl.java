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
import io.trino.plugin.base.security.TestingSystemAccessControlContext;
import io.trino.plugin.opa.AccessControlMethodHelpers.MethodWrapper;
import io.trino.plugin.opa.AccessControlMethodHelpers.ReturningMethodWrapper;
import io.trino.plugin.opa.AccessControlMethodHelpers.ThrowingMethodWrapper;
import io.trino.plugin.opa.HttpClientUtils.InstrumentedHttpClient;
import io.trino.plugin.opa.HttpClientUtils.MockResponse;
import io.trino.plugin.opa.schema.OpaViewExpression;
import io.trino.spi.QueryId;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaRoutineName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnSchema;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.SystemAccessControlFactory;
import io.trino.spi.security.SystemSecurityContext;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.security.ViewExpression;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.opa.RequestTestUtilities.assertStringRequestsEqual;
import static io.trino.plugin.opa.RequestTestUtilities.buildValidatingRequestHandler;
import static io.trino.plugin.opa.TestConstants.BAD_REQUEST_RESPONSE;
import static io.trino.plugin.opa.TestConstants.MALFORMED_RESPONSE;
import static io.trino.plugin.opa.TestConstants.NO_ACCESS_RESPONSE;
import static io.trino.plugin.opa.TestConstants.OK_RESPONSE;
import static io.trino.plugin.opa.TestConstants.OPA_COLUMN_MASKING_URI;
import static io.trino.plugin.opa.TestConstants.OPA_ROW_FILTERING_URI;
import static io.trino.plugin.opa.TestConstants.OPA_SERVER_URI;
import static io.trino.plugin.opa.TestConstants.SERVER_ERROR_RESPONSE;
import static io.trino.plugin.opa.TestConstants.TEST_COLUMN_MASKING_TABLE_NAME;
import static io.trino.plugin.opa.TestConstants.TEST_IDENTITY;
import static io.trino.plugin.opa.TestConstants.TEST_QUERY_ID;
import static io.trino.plugin.opa.TestConstants.TEST_SECURITY_CONTEXT;
import static io.trino.plugin.opa.TestConstants.UNDEFINED_RESPONSE;
import static io.trino.plugin.opa.TestConstants.columnMaskingOpaConfig;
import static io.trino.plugin.opa.TestConstants.rowFilteringOpaConfig;
import static io.trino.plugin.opa.TestConstants.simpleOpaConfig;
import static io.trino.plugin.opa.TestHelpers.assertAccessControlMethodThrowsForIllegalResponses;
import static io.trino.plugin.opa.TestHelpers.assertAccessControlMethodThrowsForResponse;
import static io.trino.plugin.opa.TestHelpers.assertAccessControlMethodThrowsForResponseHandler;
import static io.trino.plugin.opa.TestHelpers.createColumnSchema;
import static io.trino.plugin.opa.TestHelpers.createMockHttpClient;
import static io.trino.plugin.opa.TestHelpers.createOpaAuthorizer;
import static io.trino.plugin.opa.TestHelpers.createResponseHandlerForParallelColumnMasking;
import static org.assertj.core.api.Assertions.assertThat;

final class TestOpaAccessControl
{
    @Test
    void testResponseHasExtraFields()
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(
                OPA_SERVER_URI,
                buildValidatingRequestHandler(
                        TEST_IDENTITY,
                        200,
                        """
                        {
                            "result": true,
                            "decision_id": "foo",
                            "some_debug_info": {"test": ""}
                        }\
                        """));
        OpaAccessControl authorizer = createOpaAuthorizer(simpleOpaConfig(), mockClient);
        authorizer.checkCanExecuteQuery(TEST_IDENTITY, TEST_QUERY_ID);
    }

    @Test
    void testNoResourceAction()
    {
        testNoResourceAction("ExecuteQuery", (opaAccessControl, identity) -> opaAccessControl.checkCanExecuteQuery(identity, TEST_QUERY_ID));
        testNoResourceAction("ReadSystemInformation", OpaAccessControl::checkCanReadSystemInformation);
        testNoResourceAction("WriteSystemInformation", OpaAccessControl::checkCanWriteSystemInformation);
    }

    private void testNoResourceAction(String actionName, BiConsumer<OpaAccessControl, Identity> method)
    {
        Set<String> expectedRequests = ImmutableSet.of(
                """
                {
                    "operation": "%s"
                }\
                """.formatted(actionName));
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(accessControl -> method.accept(accessControl, TEST_IDENTITY));
        assertAccessControlMethodBehaviour(wrappedMethod, expectedRequests);
    }

    @Test
    void testTableResourceActions()
    {
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
        String expectedRequest =
                """
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
    void testTableWithPropertiesActions()
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
        String expectedRequest =
                """
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
    void testIdentityResourceActions()
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
                .withPrincipal(new BasicPrincipal("basic-principal"))
                .build();
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(
                accessControl -> callable.accept(accessControl, TEST_IDENTITY, dummyIdentity));

        String expectedRequest =
                """
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
    void testStringResourceAction()
    {
        testStringResourceAction("SetSystemSessionProperty", "systemSessionProperty", (accessControl, systemSecurityContext, argument) -> accessControl.checkCanSetSystemSessionProperty(systemSecurityContext.getIdentity(), TEST_QUERY_ID, argument));
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
        String expectedRequest =
                """
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
    void testCanImpersonateUser()
    {
        String expectedRequest =
                """
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
    void testCanAccessCatalog()
    {
        ReturningMethodWrapper wrappedMethod = new ReturningMethodWrapper(
                accessControl -> accessControl.canAccessCatalog(TEST_SECURITY_CONTEXT, "test_catalog"));
        String expectedRequest =
                """
                {
                    "operation": "AccessCatalog",
                    "resource": {
                        "catalog": {
                            "name": "test_catalog"
                        }
                    }
                }\
                """;
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    void testSchemaResourceActions()
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

        String expectedRequest =
                """
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
    void testCreateSchema()
    {
        CatalogSchemaName schema = new CatalogSchemaName("my_catalog", "my_schema");
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(
                accessControl -> accessControl.checkCanCreateSchema(TEST_SECURITY_CONTEXT, schema, ImmutableMap.of()));
        String expectedRequest =
                """
                {
                    "operation": "CreateSchema",
                    "resource": {
                        "schema": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "properties": {}
                        }
                    }
                }\
                """;
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    void testCreateSchemaWithProperties()
    {
        CatalogSchemaName schema = new CatalogSchemaName("my_catalog", "my_schema");
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(
                accessControl -> accessControl.checkCanCreateSchema(TEST_SECURITY_CONTEXT, schema, ImmutableMap.of("some_key", "some_value")));
        String expectedRequest =
                """
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
                }\
                """;
        assertAccessControlMethodBehaviour(wrappedMethod, ImmutableSet.of(expectedRequest));
    }

    @Test
    void testRenameSchema()
    {
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(accessControl -> accessControl.checkCanRenameSchema(
                TEST_SECURITY_CONTEXT,
                new CatalogSchemaName("my_catalog", "my_schema"), "new_schema_name"));
        String expectedRequest =
                """
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
    void testRenameTableLikeObjects()
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

        String expectedRequest =
                """
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
    void testSetSchemaAuthorization()
    {
        CatalogSchemaName schema = new CatalogSchemaName("my_catalog", "my_schema");
        TrinoPrincipal principal = new TrinoPrincipal(PrincipalType.USER, "my_user");

        ThrowingMethodWrapper methodWrapper = new ThrowingMethodWrapper(
                accessControl -> accessControl.checkCanSetSchemaAuthorization(TEST_SECURITY_CONTEXT, schema, principal));

        String expectedRequest =
                """
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
    void testSetAuthorizationOnTableLikeObjects()
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

        String expectedRequest =
                """
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
    void testColumnOperationsOnTableLikeObjects()
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
        String expectedRequest =
                """
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
    void testCanSetCatalogSessionProperty()
    {
        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(
                accessControl -> accessControl.checkCanSetCatalogSessionProperty(TEST_SECURITY_CONTEXT, "my_catalog", "my_property"));
        String expectedRequest =
                """
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
    void testFunctionResourceActions()
    {
        CatalogSchemaRoutineName routine = new CatalogSchemaRoutineName("my_catalog", "my_schema", "my_routine_name");
        String baseRequest =
                """
                {
                    "operation": "%s",
                    "resource": {
                        "function": {
                            "catalogName": "my_catalog",
                            "schemaName": "my_schema",
                            "functionName": "my_routine_name"
                        }
                    }
                }\
                """;
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
                new ThrowingMethodWrapper(authorizer -> authorizer.checkCanShowCreateFunction(TEST_SECURITY_CONTEXT, routine)),
                ImmutableSet.of(baseRequest.formatted("ShowCreateFunction")));
        assertAccessControlMethodBehaviour(
                new ReturningMethodWrapper(authorizer -> authorizer.canExecuteFunction(TEST_SECURITY_CONTEXT, routine)),
                ImmutableSet.of(baseRequest.formatted("ExecuteFunction")));
        assertAccessControlMethodBehaviour(
                new ReturningMethodWrapper(authorizer -> authorizer.canCreateViewWithExecuteFunction(TEST_SECURITY_CONTEXT, routine)),
                ImmutableSet.of(baseRequest.formatted("CreateViewWithExecuteFunction")));
    }

    @Test
    void testCanExecuteTableProcedure()
    {
        CatalogSchemaTableName table = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");
        String expectedRequest =
                """
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
                }\
                """;
        assertAccessControlMethodBehaviour(
                new ThrowingMethodWrapper(authorizer -> authorizer.checkCanExecuteTableProcedure(TEST_SECURITY_CONTEXT, table, "my_procedure")),
                ImmutableSet.of(expectedRequest));
    }

    @Test
    void testRequestContextContentsWithKnownTrinoVersion()
    {
        testRequestContextContentsForGivenTrinoVersion(
                Optional.of(new TestingSystemAccessControlContext("12345.67890")),
                "12345.67890");
    }

    @Test
    void testRequestContextContentsWithUnknownTrinoVersion()
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

        authorizer.checkCanExecuteQuery(sampleIdentityWithGroups, TEST_QUERY_ID);

        String expectedRequest =
                """
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
                }\
                """.formatted(expectedTrinoVersion);
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input");
    }

    @Test
    void testIncludeUserPrincipal() {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, request -> OK_RESPONSE);
        OpaAccessControl authorizer = (OpaAccessControl) OpaAccessControlFactory.create(
                ImmutableMap.of("opa.policy.uri", OPA_SERVER_URI.toString(), "opa.include-user-principal", "true"),
                Optional.of(mockClient),
                Optional.empty());
        Identity sampleIdentityWithGroupsAndPrincipal = Identity.forUser("test_user").withGroups(ImmutableSet.of("some_group")).withPrincipal(new SerializableTestPrincipal("test_principal")).build();
        authorizer.checkCanExecuteQuery(sampleIdentityWithGroupsAndPrincipal, TEST_QUERY_ID);

        String expectedRequest =
                """
                {
                    "action": {
                        "operation": "ExecuteQuery"
                    },
                    "context": {
                        "identity": {
                            "user": "test_user",
                            "groups": ["some_group"],
                            "principal": {
                                "name": "test_principal"
                            }
                        },
                        "softwareStack": {
                            "trinoVersion": "UNKNOWN"
                        }
                    }
                }\
                """;
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), mockClient.getRequests(), "/input");
    }

    @Test
    void testGetRowFiltersThrowsForIllegalResponse()
    {
        Consumer<OpaAccessControl> methodUnderTest = authorizer -> authorizer.getRowFilters(TEST_SECURITY_CONTEXT, TEST_COLUMN_MASKING_TABLE_NAME);
        assertAccessControlMethodThrowsForIllegalResponses(methodUnderTest, rowFilteringOpaConfig(), OPA_ROW_FILTERING_URI);

        // Also test a valid JSON response, but containing invalid fields for a row filters request
        String validJsonButIllegalSchemaResponseContents =
                """
                {
                    "result": ["some-expr"]
                }\
                """;
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
    void testGetRowFilters()
    {
        // This example is a bit strange - an undefined policy would in most cases
        // result in an access denied situation. However, since this is row-level-filtering
        // we will accept this as meaning there are no known filters to be applied.
        testGetRowFilters("{}", ImmutableList.of());

        String noExpressionsResponse =
                """
                {
                    "result": []
                }\
                """;
        testGetRowFilters(noExpressionsResponse, ImmutableList.of());

        String singleExpressionResponse =
                """
                {
                    "result": [
                        {"expression": "expr1"}
                    ]
                }\
                """;
        testGetRowFilters(
                singleExpressionResponse,
                ImmutableList.of(new OpaViewExpression("expr1", Optional.empty())));

        String multipleExpressionsAndIdentitiesResponse =
                """
                {
                    "result": [
                        {"expression": "expr1"},
                        {"expression": "expr2", "identity": "expr2_identity"},
                        {"expression": "expr3", "identity": "expr3_identity"}
                    ]
                }\
                """;
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

        List<ViewExpression> result = authorizer.getRowFilters(TEST_SECURITY_CONTEXT, TEST_COLUMN_MASKING_TABLE_NAME);
        assertThat(result).allSatisfy(expression -> {
            assertThat(expression.getCatalog()).contains("some_catalog");
            assertThat(expression.getSchema()).contains("some_schema");
        });
        assertThat(result).map(
                        viewExpression -> new OpaViewExpression(
                                viewExpression.getExpression(),
                                viewExpression.getSecurityIdentity()))
                .containsExactlyInAnyOrderElementsOf(expectedExpressions);

        String expectedRequest = String.format(
                """
                {
                    "operation": "GetRowFilters",
                    "resource": {
                        "table": {
                            "catalogName": "%s",
                            "schemaName": "%s",
                            "tableName": "%s"
                        }
                    }
                }\
                """,
                TEST_COLUMN_MASKING_TABLE_NAME.getCatalogName(),
                TEST_COLUMN_MASKING_TABLE_NAME.getSchemaTableName().getSchemaName(),
                TEST_COLUMN_MASKING_TABLE_NAME.getSchemaTableName().getTableName());
        assertStringRequestsEqual(ImmutableSet.of(expectedRequest), httpClient.getRequests(), "/input/action");
    }

    @Test
    void testGetRowFiltersDoesNothingIfNotConfigured()
    {
        InstrumentedHttpClient httpClient = createMockHttpClient(
                OPA_SERVER_URI,
                request -> {
                    throw new AssertionError("Should not have been called");
                });
        OpaAccessControl authorizer = createOpaAuthorizer(simpleOpaConfig(), httpClient);

        List<ViewExpression> result = authorizer.getRowFilters(TEST_SECURITY_CONTEXT, TEST_COLUMN_MASKING_TABLE_NAME);
        assertThat(result).isEmpty();
        assertThat(httpClient.getRequests()).isEmpty();
    }

    /**
     * `SystemAccessControl#getColumnMask` is deprecated in favour of `getColumnMasks`.
     * We don't implement this function, it is provided as a default by the interface.
     * We test that it is a no-op if called.
     */
    @Test
    void testGetColumnMaskDoesNothing()
    {
        InstrumentedHttpClient httpClient = createMockHttpClient(
                OPA_SERVER_URI,
                _ -> {
                    throw new AssertionError("Should not have been called");
                });
        OpaAccessControl authorizer = createOpaAuthorizer(simpleOpaConfig(), httpClient);

        Optional<ViewExpression> result = authorizer.getColumnMask(TEST_SECURITY_CONTEXT, TEST_COLUMN_MASKING_TABLE_NAME, "some_column", VarcharType.VARCHAR);
        assertThat(result).isEmpty();
        assertThat(httpClient.getRequests()).isEmpty();
    }

    @Test
    void testGetColumnMasks()
    {
        testGetColumnMasks(ImmutableMap.of(createColumnSchema("some-column"), "{}"), ImmutableMap.of());

        String nullResponse =
                """
                {
                    "result": null
                }\
                """;
        testGetColumnMasks(ImmutableMap.of(createColumnSchema("some-column"), nullResponse), ImmutableMap.of());

        Map<ColumnSchema, String> expressionWithoutIdentityResponses = IntStream.range(1, 10)
                .mapToObj(index -> Map.entry(
                        createColumnSchema(String.format("some-column-%d", index)),
                        String.format(
                                """
                                {
                                    "result": {"expression": "expression-%d"}
                                }\
                                """, index)))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        testGetColumnMasks(
                expressionWithoutIdentityResponses,
                IntStream.range(1, 10).mapToObj(index -> Map.entry(
                        createColumnSchema(String.format("some-column-%d", index)),
                        new OpaViewExpression(String.format("expression-%d", index), Optional.empty())
                )).collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));

        Map<ColumnSchema, String> expressionWithIdentityResponses = IntStream.range(1, 10)
                .mapToObj(index -> Map.entry(
                        createColumnSchema(String.format("some-column-%d", index)),
                        String.format(
                                """
                                {
                                    "result": {"expression": "expression-%1$d", "identity": "some_identity-%1$d"}
                                }\
                                """, index)))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        testGetColumnMasks(
                expressionWithIdentityResponses,
                IntStream.range(1, 10).mapToObj(index -> Map.entry(
                        createColumnSchema(String.format("some-column-%d", index)),
                        new OpaViewExpression(String.format("expression-%d", index), Optional.of(String.format("some_identity-%d", index)))
                )).collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));

        Map<ColumnSchema, String> mixedExpressions = ImmutableMap.of(
                createColumnSchema("some-column-1"), "{}",
                createColumnSchema("some-column-2"), nullResponse,
                createColumnSchema("some-column-3"),
                """
                {
                    "result": {"expression": "expression-1"}
                }\
                """,
                createColumnSchema("some-column-4"),
                """
                {
                    "result": {"expression": "expression-2", "identity": "some_identity-1"}
                }\
                """);
        testGetColumnMasks(
                mixedExpressions,
                ImmutableMap.of(
                        createColumnSchema("some-column-3"), new OpaViewExpression("expression-1", Optional.empty()),
                        createColumnSchema("some-column-4"), new OpaViewExpression("expression-2", Optional.of("some_identity-1"))));
    }

    @Test
    void testGetColumnMasksDoesNothingIfNotConfigured()
    {
        InstrumentedHttpClient httpClient = createMockHttpClient(
                OPA_SERVER_URI,
                request -> {
                    throw new AssertionError("Should not have been called");
                });

        OpaAccessControl authorizer = createOpaAuthorizer(simpleOpaConfig(), httpClient);

        Map<ColumnSchema, ViewExpression> result = authorizer.getColumnMasks(TEST_SECURITY_CONTEXT, TEST_COLUMN_MASKING_TABLE_NAME,
                Stream.of("some_column_1", "another_column_2").map(TestHelpers::createColumnSchema).collect(toImmutableList()));
        assertThat(result).isEmpty();
        assertThat(httpClient.getRequests()).isEmpty();
    }

    @Test
    void testGetColumnMasksThrowsForIllegalResponse()
    {
        OpaConfig opaConfig = columnMaskingOpaConfig();

        List<ColumnSchema> tableColumnSchemas = Stream.of("some_column_1", "other_column_2", "illegal_response_column").map(TestHelpers::createColumnSchema).collect(toImmutableList());
        Consumer<OpaAccessControl> methodUnderTest = authorizer -> authorizer.getColumnMasks(TEST_SECURITY_CONTEXT, TEST_COLUMN_MASKING_TABLE_NAME, tableColumnSchemas);
        assertAccessControlMethodThrowsForIllegalResponses(methodUnderTest, opaConfig, OPA_COLUMN_MASKING_URI);

        // Test invalid JSON response for just one of the columns
        assertAccessControlMethodThrowsForResponseHandler(
                createResponseHandlerForParallelColumnMasking(ImmutableMap.of(createColumnSchema("illegal_response_column"), UNDEFINED_RESPONSE)),
                OPA_COLUMN_MASKING_URI, opaConfig, methodUnderTest, OpaQueryException.OpaServerError.PolicyNotFound.class, "did not return a value");
        assertAccessControlMethodThrowsForResponseHandler(
                createResponseHandlerForParallelColumnMasking(ImmutableMap.of(createColumnSchema("illegal_response_column"), BAD_REQUEST_RESPONSE)), OPA_COLUMN_MASKING_URI, opaConfig, methodUnderTest, OpaQueryException.OpaServerError.class, "returned status 400");
        assertAccessControlMethodThrowsForResponseHandler(
                createResponseHandlerForParallelColumnMasking(ImmutableMap.of(createColumnSchema("illegal_response_column"), SERVER_ERROR_RESPONSE)), OPA_COLUMN_MASKING_URI, opaConfig, methodUnderTest, OpaQueryException.OpaServerError.class, "returned status 500");
        assertAccessControlMethodThrowsForResponseHandler(
                createResponseHandlerForParallelColumnMasking(ImmutableMap.of(createColumnSchema("illegal_response_column"), MALFORMED_RESPONSE)), OPA_COLUMN_MASKING_URI, opaConfig, methodUnderTest, OpaQueryException.class, "Failed to deserialize");

        // Also test a valid JSON response that contains invalid fields
        String validJsonButIllegalSchemaResponseContents =
                """
                {
                    "result": {"expression": {"foo": "bar"}}
                }\
                """;
        MockResponse response = new MockResponse(validJsonButIllegalSchemaResponseContents, 200);
        assertAccessControlMethodThrowsForResponse(
                response,
                OPA_COLUMN_MASKING_URI,
                columnMaskingOpaConfig(),
                methodUnderTest,
                OpaQueryException.class,
                "Failed to deserialize");

        // Same test with only one column having the valid but illegal JSON response
        assertAccessControlMethodThrowsForResponseHandler(
                createResponseHandlerForParallelColumnMasking(ImmutableMap.of(createColumnSchema("illegal_response_column"), response)),
                OPA_COLUMN_MASKING_URI,
                opaConfig,
                methodUnderTest,
                OpaQueryException.class,
                "Failed to deserialize");
    }


    @Test
    public void testQueryIdPropagation()
    {
        QueryId queryId = new QueryId("20250718_081710_03427_trino");

        SystemSecurityContext customSecurityContext = new SystemSecurityContext(TEST_IDENTITY, queryId, Instant.now());
        CatalogSchemaTableName tableName = new CatalogSchemaTableName("my_catalog", "my_schema", "my_table");

        ThrowingMethodWrapper wrappedMethod = new ThrowingMethodWrapper(accessControl ->
                accessControl.checkCanShowCreateTable(customSecurityContext, tableName));

        String expectedActionRequest =
                """
                {
                    "operation": "ShowCreateTable",
                    "resource": {
                        "table": {
                            "catalogName": "%s",
                            "schemaName": "%s",
                            "tableName": "%s"
                        }
                    }
                }
                """.formatted(
                        tableName.getCatalogName(),
                        tableName.getSchemaTableName().getSchemaName(),
                        tableName.getSchemaTableName().getTableName());

        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, request -> {
            JsonNode contextNode = request.path("input").path("context");

            assertThat(contextNode.path("queryId").asText()).isEqualTo(queryId.id());
            assertThat(contextNode.path("identity").path("user").asText()).isEqualTo(TEST_IDENTITY.getUser());
            assertThat(contextNode.path("softwareStack").path("trinoVersion").asText()).isEqualTo("trino-version");

            return OK_RESPONSE;
        });

        OpaAccessControl authorizer = createOpaAuthorizer(simpleOpaConfig(), mockClient);

        assertThat(wrappedMethod.isAccessAllowed(authorizer)).isTrue();
        assertStringRequestsEqual(ImmutableSet.of(expectedActionRequest), mockClient.getRequests(), "/input/action");
    }

    private void testGetColumnMasks(Map<ColumnSchema, String> columnResponseContent, Map<ColumnSchema, OpaViewExpression> expectedResult)
    {
        InstrumentedHttpClient httpClient = createMockHttpClient(
                OPA_COLUMN_MASKING_URI,
                buildValidatingRequestHandler(TEST_IDENTITY, createResponseHandlerForParallelColumnMasking(columnResponseContent.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, entry -> new MockResponse(entry.getValue(), 200))))));
        OpaAccessControl authorizer = createOpaAuthorizer(columnMaskingOpaConfig(), httpClient);

        Map<ColumnSchema, ViewExpression> result = authorizer.getColumnMasks(TEST_SECURITY_CONTEXT, TEST_COLUMN_MASKING_TABLE_NAME, ImmutableList.copyOf(columnResponseContent.keySet()));

        assertColumnMaskBehaviour(columnResponseContent.keySet().stream().map(ColumnSchema::getName).collect(toImmutableList()), result, expectedResult, httpClient.getRequests());
    }

    private void assertColumnMaskBehaviour(List<String> columnNames, Map<ColumnSchema, ViewExpression> actualResult, Map<ColumnSchema, OpaViewExpression> expectedResult, List<JsonNode> requests)
    {
        assertThat(actualResult.entrySet().stream().map(entry -> {
            ViewExpression viewExpression = entry.getValue();
            assertThat(viewExpression.getCatalog()).contains(TEST_COLUMN_MASKING_TABLE_NAME.getCatalogName());
            assertThat(viewExpression.getSchema()).contains(TEST_COLUMN_MASKING_TABLE_NAME.getSchemaTableName().getSchemaName());
            return Map.entry(entry.getKey(), new OpaViewExpression(viewExpression.getExpression(), viewExpression.getSecurityIdentity()));
        })).containsExactlyInAnyOrderElementsOf(expectedResult.entrySet());

        Set<String> expectedRequests = columnNames.stream().map(columnName -> String.format(
                """
                {
                    "operation": "GetColumnMask",
                    "resource": {
                        "column": {
                            "catalogName": "%s",
                            "schemaName": "%s",
                            "tableName": "%s",
                            "columnName": "%s",
                            "columnType": "varchar"
                        }
                    }
                }\
                """,
                TEST_COLUMN_MASKING_TABLE_NAME.getCatalogName(),
                TEST_COLUMN_MASKING_TABLE_NAME.getSchemaTableName().getSchemaName(),
                TEST_COLUMN_MASKING_TABLE_NAME.getSchemaTableName().getTableName(),
                columnName)).collect(toImmutableSet());
        assertStringRequestsEqual(expectedRequests, requests, "/input/action");
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
