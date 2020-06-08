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
package io.prestosql.plugin.base.security;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemSecurityContext;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.testng.annotations.Test;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.io.File;
import java.util.Optional;
import java.util.Set;

import static com.google.common.io.Files.copy;
import static io.prestosql.plugin.base.security.FileBasedAccessControlConfig.SECURITY_CONFIG_FILE;
import static io.prestosql.plugin.base.security.FileBasedAccessControlConfig.SECURITY_REFRESH_PERIOD;
import static io.prestosql.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Files.newTemporaryFile;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestFileBasedSystemAccessControl
{
    private static final Identity alice = Identity.forUser("alice").withGroups(ImmutableSet.of("staff")).build();
    private static final Identity kerberosValidAlice = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("alice/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosValidNonAsciiUser = Identity.forUser("\u0194\u0194\u0194").withPrincipal(new KerberosPrincipal("\u0194\u0194\u0194/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosInvalidAlice = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("mallory/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosValidShare = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("valid/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosInValidShare = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("invalid/example.com@EXAMPLE.COM")).build();
    private static final Identity validSpecialRegexWildDot = Identity.forUser(".*").withPrincipal(new KerberosPrincipal("special/.*@EXAMPLE.COM")).build();
    private static final Identity validSpecialRegexEndQuote = Identity.forUser("\\E").withPrincipal(new KerberosPrincipal("special/\\E@EXAMPLE.COM")).build();
    private static final Identity invalidSpecialRegex = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("special/.*@EXAMPLE.COM")).build();
    private static final Identity bob = Identity.forUser("bob").withGroups(ImmutableSet.of("staff")).build();
    private static final Identity admin = Identity.forUser("admin").withGroups(ImmutableSet.of("admin", "staff")).build();
    private static final Identity nonAsciiUser = Identity.ofUser("\u0194\u0194\u0194");
    private static final Set<String> allCatalogs = ImmutableSet.of("secret", "open-to-all", "all-allowed", "alice-catalog", "allowed-absent", "\u0200\u0200\u0200");
    private static final CatalogSchemaTableName aliceView = new CatalogSchemaTableName("alice-catalog", "schema", "view");
    private static final Optional<QueryId> queryId = Optional.empty();

    private static final Identity charlie = Identity.forUser("charlie").withGroups(ImmutableSet.of("guests")).build();
    private static final Identity joe = Identity.ofUser("joe");
    private static final SystemSecurityContext ADMIN = new SystemSecurityContext(admin, queryId);
    private static final SystemSecurityContext BOB = new SystemSecurityContext(bob, queryId);
    private static final SystemSecurityContext CHARLIE = new SystemSecurityContext(charlie, queryId);

    private static final String DROP_SCHEMA_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot drop schema .*";
    private static final String RENAME_SCHEMA_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot rename schema from .* to .*";
    private static final String AUTH_SCHEMA_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot set authorization for schema .* to .*";
    private static final String SHOW_CREATE_SCHEMA_ACCESS_DENIED_MESSAGE = "Access Denied: Cannot show create schema for .*";

    @Test
    public void testSchemaRulesForCheckCanDropSchema()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-schema.json");

        accessControl.checkCanDropSchema(ADMIN, new CatalogSchemaName("some-catalog", "bob"));
        accessControl.checkCanDropSchema(ADMIN, new CatalogSchemaName("some-catalog", "staff"));
        accessControl.checkCanDropSchema(ADMIN, new CatalogSchemaName("some-catalog", "authenticated"));
        accessControl.checkCanDropSchema(ADMIN, new CatalogSchemaName("some-catalog", "test"));

        accessControl.checkCanDropSchema(BOB, new CatalogSchemaName("some-catalog", "bob"));
        accessControl.checkCanDropSchema(BOB, new CatalogSchemaName("some-catalog", "staff"));
        accessControl.checkCanDropSchema(BOB, new CatalogSchemaName("some-catalog", "authenticated"));
        assertAccessDenied(() -> accessControl.checkCanDropSchema(BOB, new CatalogSchemaName("some-catalog", "test")), DROP_SCHEMA_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanDropSchema(CHARLIE, new CatalogSchemaName("some-catalog", "authenticated"));
        assertAccessDenied(() -> accessControl.checkCanDropSchema(CHARLIE, new CatalogSchemaName("some-catalog", "bob")), DROP_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanDropSchema(CHARLIE, new CatalogSchemaName("some-catalog", "staff")), DROP_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanDropSchema(CHARLIE, new CatalogSchemaName("some-catalog", "test")), DROP_SCHEMA_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testSchemaRulesForCheckCanRenameSchema()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-schema.json");

        accessControl.checkCanRenameSchema(ADMIN, new CatalogSchemaName("some-catalog", "bob"), "new_schema");
        accessControl.checkCanRenameSchema(ADMIN, new CatalogSchemaName("some-catalog", "staff"), "new_schema");
        accessControl.checkCanRenameSchema(ADMIN, new CatalogSchemaName("some-catalog", "authenticated"), "new_schema");
        accessControl.checkCanRenameSchema(ADMIN, new CatalogSchemaName("some-catalog", "test"), "new_schema");

        accessControl.checkCanRenameSchema(BOB, new CatalogSchemaName("some-catalog", "bob"), "staff");
        accessControl.checkCanRenameSchema(BOB, new CatalogSchemaName("some-catalog", "staff"), "authenticated");
        accessControl.checkCanRenameSchema(BOB, new CatalogSchemaName("some-catalog", "authenticated"), "bob");
        assertAccessDenied(() -> accessControl.checkCanRenameSchema(BOB, new CatalogSchemaName("some-catalog", "test"), "bob"), RENAME_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanRenameSchema(BOB, new CatalogSchemaName("some-catalog", "bob"), "test"), RENAME_SCHEMA_ACCESS_DENIED_MESSAGE);

        assertAccessDenied(() -> accessControl.checkCanRenameSchema(CHARLIE, new CatalogSchemaName("some-catalog", "bob"), "new_schema"), RENAME_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanRenameSchema(CHARLIE, new CatalogSchemaName("some-catalog", "staff"), "new_schema"), RENAME_SCHEMA_ACCESS_DENIED_MESSAGE);
        accessControl.checkCanRenameSchema(CHARLIE, new CatalogSchemaName("some-catalog", "authenticated"), "authenticated");
        assertAccessDenied(() -> accessControl.checkCanRenameSchema(CHARLIE, new CatalogSchemaName("some-catalog", "test"), "new_schema"), RENAME_SCHEMA_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testSchemaRulesForCheckCanSetSchemaAuthorization()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-schema.json");

        accessControl.checkCanSetSchemaAuthorization(ADMIN, new CatalogSchemaName("some-catalog", "test"), new PrestoPrincipal(PrincipalType.ROLE, "some_role"));
        accessControl.checkCanSetSchemaAuthorization(ADMIN, new CatalogSchemaName("some-catalog", "test"), new PrestoPrincipal(PrincipalType.USER, "some_user"));
        accessControl.checkCanSetSchemaAuthorization(BOB, new CatalogSchemaName("some-catalog", "bob"), new PrestoPrincipal(PrincipalType.ROLE, "some_role"));
        accessControl.checkCanSetSchemaAuthorization(BOB, new CatalogSchemaName("some-catalog", "bob"), new PrestoPrincipal(PrincipalType.USER, "some_user"));
        assertAccessDenied(() -> accessControl.checkCanSetSchemaAuthorization(BOB, new CatalogSchemaName("some-catalog", "test"), new PrestoPrincipal(PrincipalType.ROLE, "some_role")), AUTH_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanSetSchemaAuthorization(BOB, new CatalogSchemaName("some-catalog", "test"), new PrestoPrincipal(PrincipalType.USER, "some_user")), AUTH_SCHEMA_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testSchemaRulesForCheckCanShowCreateSchema()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("file-based-system-access-schema.json");

        accessControl.checkCanShowCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "bob"));
        accessControl.checkCanShowCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "staff"));
        accessControl.checkCanShowCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "authenticated"));
        accessControl.checkCanShowCreateSchema(ADMIN, new CatalogSchemaName("some-catalog", "test"));

        accessControl.checkCanShowCreateSchema(BOB, new CatalogSchemaName("some-catalog", "bob"));
        accessControl.checkCanShowCreateSchema(BOB, new CatalogSchemaName("some-catalog", "staff"));
        accessControl.checkCanShowCreateSchema(BOB, new CatalogSchemaName("some-catalog", "authenticated"));
        assertAccessDenied(() -> accessControl.checkCanShowCreateSchema(BOB, new CatalogSchemaName("some-catalog", "test")), SHOW_CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);

        accessControl.checkCanShowCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "authenticated"));
        assertAccessDenied(() -> accessControl.checkCanShowCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "bob")), SHOW_CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "staff")), SHOW_CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);
        assertAccessDenied(() -> accessControl.checkCanShowCreateSchema(CHARLIE, new CatalogSchemaName("some-catalog", "test")), SHOW_CREATE_SCHEMA_ACCESS_DENIED_MESSAGE);
    }

    @Test
    public void testCanSetUserOperations()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("catalog_principal.json");

        try {
            accessControl.checkCanSetUser(Optional.empty(), alice.getUser());
            throw new AssertionError("expected AccessDeniedException");
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanSetUser(kerberosValidAlice.getPrincipal(), kerberosValidAlice.getUser());
        accessControl.checkCanSetUser(kerberosValidNonAsciiUser.getPrincipal(), kerberosValidNonAsciiUser.getUser());
        try {
            accessControl.checkCanSetUser(kerberosInvalidAlice.getPrincipal(), kerberosInvalidAlice.getUser());
            throw new AssertionError("expected AccessDeniedException");
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanSetUser(kerberosValidShare.getPrincipal(), kerberosValidShare.getUser());
        try {
            accessControl.checkCanSetUser(kerberosInValidShare.getPrincipal(), kerberosInValidShare.getUser());
            throw new AssertionError("expected AccessDeniedException");
        }
        catch (AccessDeniedException expected) {
        }

        accessControl.checkCanSetUser(validSpecialRegexWildDot.getPrincipal(), validSpecialRegexWildDot.getUser());
        accessControl.checkCanSetUser(validSpecialRegexEndQuote.getPrincipal(), validSpecialRegexEndQuote.getUser());
        try {
            accessControl.checkCanSetUser(invalidSpecialRegex.getPrincipal(), invalidSpecialRegex.getUser());
            throw new AssertionError("expected AccessDeniedException");
        }
        catch (AccessDeniedException expected) {
        }

        SystemAccessControl accessControlNoPatterns = newFileBasedSystemAccessControl("catalog.json");
        accessControlNoPatterns.checkCanSetUser(kerberosValidAlice.getPrincipal(), kerberosValidAlice.getUser());
    }

    @Test
    public void testQuery()
    {
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl("query.json");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(admin, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(admin, queryId), "any");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(admin, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of("a", "b"));
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(admin, queryId), "any");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(alice, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(alice, queryId), "any");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(alice, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of("a", "b"));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(alice, queryId), "any"));

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(bob, queryId)));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(bob, queryId), "any"));
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(bob, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of());
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(bob, queryId), "any");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(nonAsciiUser, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(nonAsciiUser, queryId), "any");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(nonAsciiUser, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of("a", "b"));
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(nonAsciiUser, queryId), "any");
    }

    @Test
    public void testQueryNotSet()
    {
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl("catalog.json");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(bob, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(bob, queryId), "any");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(bob, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of("a", "b"));
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(bob, queryId), "any");
    }

    @Test
    public void testDocsExample()
    {
        String rulesFile = new File("../presto-docs/src/main/sphinx/security/query-access.json").getAbsolutePath();
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl(ImmutableMap.of("security.config-file", rulesFile));

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(admin, queryId));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(admin, queryId), "any");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(admin, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of("a", "b"));
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(admin, queryId), "any");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(alice, queryId));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(alice, queryId), "any"));
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(alice, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of());
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(alice, queryId), "any");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(bob, queryId));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(bob, queryId), "any"));
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(bob, queryId), ImmutableSet.of("a", "b")), ImmutableSet.of());
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(bob, queryId), "any"));
    }

    @Test
    public void testSchemaOperations()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("catalog.json");

        PrestoPrincipal user = new PrestoPrincipal(PrincipalType.USER, "some_user");
        PrestoPrincipal role = new PrestoPrincipal(PrincipalType.ROLE, "some_user");

        accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(admin, queryId), new CatalogSchemaName("alice-catalog", "some_schema"), user);
        accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(admin, queryId), new CatalogSchemaName("alice-catalog", "some_schema"), role);

        accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(alice, queryId), new CatalogSchemaName("alice-catalog", "some_schema"), user);
        accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(alice, queryId), new CatalogSchemaName("alice-catalog", "some_schema"), role);

        assertThatThrownBy(() -> accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(bob, queryId), new CatalogSchemaName("alice-catalog", "some_schema"), user))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageStartingWith("Access Denied: Cannot set authorization for schema alice-catalog.some_schema");

        assertThatThrownBy(() -> accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(bob, queryId), new CatalogSchemaName("alice-catalog", "some_schema"), role))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageStartingWith("Access Denied: Cannot set authorization for schema alice-catalog.some_schema");

        assertThatThrownBy(() -> accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(alice, queryId), new CatalogSchemaName("secret", "some_schema"), user))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageStartingWith("Access Denied: Cannot set authorization for schema secret.some_schema");

        assertThatThrownBy(() -> accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(alice, queryId), new CatalogSchemaName("secret", "some_schema"), role))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageStartingWith("Access Denied: Cannot set authorization for schema secret.some_schema");
    }

    @Test
    public void testCatalogOperations()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("catalog.json");

        assertEquals(accessControl.filterCatalogs(new SystemSecurityContext(admin, queryId), allCatalogs), allCatalogs);
        Set<String> aliceCatalogs = ImmutableSet.of("open-to-all", "alice-catalog", "all-allowed");
        assertEquals(accessControl.filterCatalogs(new SystemSecurityContext(alice, queryId), allCatalogs), aliceCatalogs);
        Set<String> bobCatalogs = ImmutableSet.of("open-to-all", "all-allowed");
        assertEquals(accessControl.filterCatalogs(new SystemSecurityContext(bob, queryId), allCatalogs), bobCatalogs);
        Set<String> nonAsciiUserCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "\u0200\u0200\u0200");
        assertEquals(accessControl.filterCatalogs(new SystemSecurityContext(nonAsciiUser, queryId), allCatalogs), nonAsciiUserCatalogs);
    }

    @Test
    public void testEverythingImplemented()
    {
        assertAllMethodsOverridden(SystemAccessControl.class, FileBasedSystemAccessControl.class);
    }

    @Test
    public void testRefreshing()
            throws Exception
    {
        File configFile = newTemporaryFile();
        configFile.deleteOnExit();
        copy(new File(getResourcePath("catalog.json")), configFile);

        SystemAccessControl accessControl = newFileBasedSystemAccessControl(ImmutableMap.of(
                SECURITY_CONFIG_FILE, configFile.getAbsolutePath(),
                SECURITY_REFRESH_PERIOD, "1ms"));

        SystemSecurityContext alice = new SystemSecurityContext(TestFileBasedSystemAccessControl.alice, queryId);
        accessControl.checkCanCreateView(alice, aliceView);
        accessControl.checkCanCreateView(alice, aliceView);
        accessControl.checkCanCreateView(alice, aliceView);

        copy(new File(getResourcePath("security-config-file-with-unknown-rules.json")), configFile);
        sleep(2);

        assertThatThrownBy(() -> accessControl.checkCanCreateView(alice, aliceView))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid JSON file");

        // test if file based cached control was not cached somewhere
        assertThatThrownBy(() -> accessControl.checkCanCreateView(alice, aliceView))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Invalid JSON file");

        copy(new File(getResourcePath("catalog.json")), configFile);
        sleep(2);

        accessControl.checkCanCreateView(alice, aliceView);
    }

    @Test
    public void parseUnknownRules()
    {
        assertThatThrownBy(() -> newFileBasedSystemAccessControl("security-config-file-with-unknown-rules.json"))
                .hasMessageContaining("Invalid JSON");
    }

    private SystemAccessControl newFileBasedSystemAccessControl(String resourceName)
    {
        return newFileBasedSystemAccessControl(ImmutableMap.of("security.config-file", getResourcePath(resourceName)));
    }

    private SystemAccessControl newFileBasedSystemAccessControl(ImmutableMap<String, String> config)
    {
        return new FileBasedSystemAccessControl.Factory().create(config);
    }

    private String getResourcePath(String resourceName)
    {
        return this.getClass().getClassLoader().getResource(resourceName).getPath();
    }

    private static void assertAccessDenied(ThrowingCallable callable, String expectedMessage)
    {
        assertThatThrownBy(callable)
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageMatching(expectedMessage);
    }
}
