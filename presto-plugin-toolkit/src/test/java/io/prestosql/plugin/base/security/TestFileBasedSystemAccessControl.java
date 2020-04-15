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
import io.prestosql.spi.connector.CatalogSchemaName;
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemSecurityContext;
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
    private static final Identity alice = Identity.ofUser("alice");
    private static final Identity kerberosValidAlice = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("alice/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosValidNonAsciiUser = Identity.forUser("\u0194\u0194\u0194").withPrincipal(new KerberosPrincipal("\u0194\u0194\u0194/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosInvalidAlice = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("mallory/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosValidShare = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("valid/example.com@EXAMPLE.COM")).build();
    private static final Identity kerberosInValidShare = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("invalid/example.com@EXAMPLE.COM")).build();
    private static final Identity validSpecialRegexWildDot = Identity.forUser(".*").withPrincipal(new KerberosPrincipal("special/.*@EXAMPLE.COM")).build();
    private static final Identity validSpecialRegexEndQuote = Identity.forUser("\\E").withPrincipal(new KerberosPrincipal("special/\\E@EXAMPLE.COM")).build();
    private static final Identity invalidSpecialRegex = Identity.forUser("alice").withPrincipal(new KerberosPrincipal("special/.*@EXAMPLE.COM")).build();
    private static final Identity bob = Identity.ofUser("bob");
    private static final Identity admin = Identity.ofUser("admin");
    private static final Identity nonAsciiUser = Identity.ofUser("\u0194\u0194\u0194");
    private static final Set<String> allCatalogs = ImmutableSet.of("secret", "open-to-all", "all-allowed", "alice-catalog", "allowed-absent", "\u0200\u0200\u0200");
    private static final CatalogSchemaTableName aliceView = new CatalogSchemaTableName("alice-catalog", "schema", "view");

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

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(admin));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(admin), "any");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(admin), ImmutableSet.of("a", "b")), ImmutableSet.of("a", "b"));
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(admin), "any");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(alice));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(alice), "any");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(alice), ImmutableSet.of("a", "b")), ImmutableSet.of("a", "b"));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(alice), "any"));

        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(bob)));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(bob), "any"));
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(bob), ImmutableSet.of("a", "b")), ImmutableSet.of());
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(bob), "any");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(nonAsciiUser));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(nonAsciiUser), "any");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(nonAsciiUser), ImmutableSet.of("a", "b")), ImmutableSet.of("a", "b"));
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(nonAsciiUser), "any");
    }

    @Test
    public void testQueryNotSet()
    {
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl("catalog.json");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(bob));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(bob), "any");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(bob), ImmutableSet.of("a", "b")), ImmutableSet.of("a", "b"));
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(bob), "any");
    }

    @Test
    public void testDocsExample()
    {
        String rulesFile = new File("../presto-docs/src/main/sphinx/security/query-access.json").getAbsolutePath();
        SystemAccessControl accessControlManager = newFileBasedSystemAccessControl(ImmutableMap.of("security.config-file", rulesFile));

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(admin));
        accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(admin), "any");
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(admin), ImmutableSet.of("a", "b")), ImmutableSet.of("a", "b"));
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(admin), "any");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(alice));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(alice), "any"));
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(alice), ImmutableSet.of("a", "b")), ImmutableSet.of());
        accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(alice), "any");

        accessControlManager.checkCanExecuteQuery(new SystemSecurityContext(bob));
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanViewQueryOwnedBy(new SystemSecurityContext(bob), "any"));
        assertEquals(accessControlManager.filterViewQueryOwnedBy(new SystemSecurityContext(bob), ImmutableSet.of("a", "b")), ImmutableSet.of());
        assertThrows(AccessDeniedException.class, () -> accessControlManager.checkCanKillQueryOwnedBy(new SystemSecurityContext(bob), "any"));
    }

    @Test
    public void testSchemaOperations()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("catalog.json");

        PrestoPrincipal user = new PrestoPrincipal(PrincipalType.USER, "some_user");
        PrestoPrincipal role = new PrestoPrincipal(PrincipalType.ROLE, "some_user");

        accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(admin), new CatalogSchemaName("alice-catalog", "some_schema"), user);
        accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(admin), new CatalogSchemaName("alice-catalog", "some_schema"), role);

        accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(alice), new CatalogSchemaName("alice-catalog", "some_schema"), user);
        accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(alice), new CatalogSchemaName("alice-catalog", "some_schema"), role);

        assertThatThrownBy(() -> accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(bob), new CatalogSchemaName("alice-catalog", "some_schema"), user))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageStartingWith("Access Denied: Cannot set authorization for schema alice-catalog.some_schema");

        assertThatThrownBy(() -> accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(bob), new CatalogSchemaName("alice-catalog", "some_schema"), role))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageStartingWith("Access Denied: Cannot set authorization for schema alice-catalog.some_schema");

        assertThatThrownBy(() -> accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(alice), new CatalogSchemaName("secret", "some_schema"), user))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageStartingWith("Access Denied: Cannot set authorization for schema secret.some_schema");

        assertThatThrownBy(() -> accessControl.checkCanSetSchemaAuthorization(new SystemSecurityContext(alice), new CatalogSchemaName("secret", "some_schema"), role))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageStartingWith("Access Denied: Cannot set authorization for schema secret.some_schema");
    }

    @Test
    public void testCatalogOperations()
    {
        SystemAccessControl accessControl = newFileBasedSystemAccessControl("catalog.json");

        assertEquals(accessControl.filterCatalogs(new SystemSecurityContext(admin), allCatalogs), allCatalogs);
        Set<String> aliceCatalogs = ImmutableSet.of("open-to-all", "alice-catalog", "all-allowed");
        assertEquals(accessControl.filterCatalogs(new SystemSecurityContext(alice), allCatalogs), aliceCatalogs);
        Set<String> bobCatalogs = ImmutableSet.of("open-to-all", "all-allowed");
        assertEquals(accessControl.filterCatalogs(new SystemSecurityContext(bob), allCatalogs), bobCatalogs);
        Set<String> nonAsciiUserCatalogs = ImmutableSet.of("open-to-all", "all-allowed", "\u0200\u0200\u0200");
        assertEquals(accessControl.filterCatalogs(new SystemSecurityContext(nonAsciiUser), allCatalogs), nonAsciiUserCatalogs);
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

        SystemSecurityContext alice = new SystemSecurityContext(TestFileBasedSystemAccessControl.alice);
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
}
