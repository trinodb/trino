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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.opa.HttpClientUtils.InstrumentedHttpClient;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static io.trino.plugin.opa.TestConstants.SYSTEM_ACCESS_CONTROL_CONTEXT;
import static io.trino.plugin.opa.TestConstants.TEST_SECURITY_CONTEXT;
import static io.trino.plugin.opa.TestHelpers.createMockHttpClient;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOpaAccessControlPermissionManagementOperations
{
    private static final URI OPA_SERVER_URI = URI.create("http://my-uri/");

    @Test
    public void testTablePrivilegeGrantingOperationsDeniedOrAllowedByConfig()
    {
        CatalogSchemaTableName sampleTableName = new CatalogSchemaTableName("some_catalog", "some_schema", "some_table");
        TrinoPrincipal samplePrincipal = new TrinoPrincipal(PrincipalType.USER, "some_user");

        testOperationAllowedOrDeniedByConfig(
                authorizer -> authorizer.checkCanGrantTablePrivilege(TEST_SECURITY_CONTEXT, Privilege.CREATE, sampleTableName, samplePrincipal, false));
        testOperationAllowedOrDeniedByConfig(
                authorizer -> authorizer.checkCanRevokeTablePrivilege(TEST_SECURITY_CONTEXT, Privilege.CREATE, sampleTableName, samplePrincipal, false));
        testOperationAllowedOrDeniedByConfig(
                authorizer -> authorizer.checkCanDenyTablePrivilege(TEST_SECURITY_CONTEXT, Privilege.CREATE, sampleTableName, samplePrincipal));
    }

    @Test
    public void testSchemaPrivilegeGrantingOperationsDeniedOrAllowedByConfig()
    {
        CatalogSchemaName sampleSchemaName = new CatalogSchemaName("some_catalog", "some_schema");
        TrinoPrincipal samplePrincipal = new TrinoPrincipal(PrincipalType.USER, "some_user");

        testOperationAllowedOrDeniedByConfig(
                authorizer -> authorizer.checkCanGrantSchemaPrivilege(TEST_SECURITY_CONTEXT, Privilege.CREATE, sampleSchemaName, samplePrincipal, false));
        testOperationAllowedOrDeniedByConfig(
                authorizer -> authorizer.checkCanRevokeSchemaPrivilege(TEST_SECURITY_CONTEXT, Privilege.CREATE, sampleSchemaName, samplePrincipal, false));
        testOperationAllowedOrDeniedByConfig(
                authorizer -> authorizer.checkCanDenySchemaPrivilege(TEST_SECURITY_CONTEXT, Privilege.CREATE, sampleSchemaName, samplePrincipal));
    }

    @Test
    public void testCanCreateRoleAllowedOrDeniedByConfig()
    {
        testOperationAllowedOrDeniedByConfig(
                authorizer -> authorizer.checkCanCreateRole(TEST_SECURITY_CONTEXT, "some_role", Optional.empty()));
    }

    @Test
    public void testCanDropRoleAllowedOrDeniedByConfig()
    {
        testOperationAllowedOrDeniedByConfig(
                authorizer -> authorizer.checkCanDropRole(TEST_SECURITY_CONTEXT, "some_role"));
    }

    @Test
    public void testCanGrantRolesAllowedOrDeniedByConfig()
    {
        Set<String> roles = ImmutableSet.of("role_one", "role_two");
        Set<TrinoPrincipal> grantees = ImmutableSet.of(new TrinoPrincipal(PrincipalType.USER, "some_principal"));
        testOperationAllowedOrDeniedByConfig(
                authorizer -> authorizer.checkCanGrantRoles(TEST_SECURITY_CONTEXT, roles, grantees, true, Optional.empty()));
    }

    @Test
    public void testShowRolesAlwaysAllowedRegardlessOfConfig()
    {
        testOperationAlwaysAllowedRegardlessOfConfig(authorizer -> authorizer.checkCanShowRoles(TEST_SECURITY_CONTEXT));
    }

    @Test
    public void testShowCurrentRolesAlwaysAllowedRegardlessOfConfig()
    {
        testOperationAlwaysAllowedRegardlessOfConfig(authorizer -> authorizer.checkCanShowCurrentRoles(TEST_SECURITY_CONTEXT));
    }

    @Test
    public void testShowRoleGrantsAlwaysAllowedRegardlessOfConfig()
    {
        testOperationAlwaysAllowedRegardlessOfConfig(authorizer -> authorizer.checkCanShowRoleGrants(TEST_SECURITY_CONTEXT));
    }

    private static void testOperationAllowedOrDeniedByConfig(Consumer<OpaAccessControl> methodToTest)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, request -> null);
        OpaAccessControl permissiveAuthorizer = createAuthorizer(true, mockClient);
        OpaAccessControl restrictiveAuthorizer = createAuthorizer(false, mockClient);

        methodToTest.accept(permissiveAuthorizer);
        assertThatThrownBy(() -> methodToTest.accept(restrictiveAuthorizer))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Access Denied:");
        assertThat(mockClient.getRequests()).isEmpty();
    }

    private static void testOperationAlwaysAllowedRegardlessOfConfig(Consumer<OpaAccessControl> methodToTest)
    {
        InstrumentedHttpClient mockClient = createMockHttpClient(OPA_SERVER_URI, request -> null);
        OpaAccessControl permissiveAuthorizer = createAuthorizer(true, mockClient);
        OpaAccessControl restrictiveAuthorizer = createAuthorizer(false, mockClient);
        methodToTest.accept(permissiveAuthorizer);
        methodToTest.accept(restrictiveAuthorizer);

        assertThat(mockClient.getRequests()).isEmpty();
    }

    private static OpaAccessControl createAuthorizer(boolean allowPermissionManagementOperations, InstrumentedHttpClient mockClient)
    {
        return (OpaAccessControl) OpaAccessControlFactory.create(
                ImmutableMap.<String, String>builder()
                        .put("opa.policy.uri", OPA_SERVER_URI.toString())
                        .put("opa.allow-permission-management-operations", String.valueOf(allowPermissionManagementOperations))
                        .buildOrThrow(),
                Optional.of(mockClient),
                Optional.of(SYSTEM_ACCESS_CONTROL_CONTEXT));
    }
}
