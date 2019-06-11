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
import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.Identity;
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

public class TestFileBasedSystemAccessControl
{
    private static final Identity alice = new Identity("alice", Optional.empty());
    private static final Identity kerberosValidAlice = new Identity("alice", Optional.of(new KerberosPrincipal("alice/example.com@EXAMPLE.COM")));
    private static final Identity kerberosValidNonAsciiUser = new Identity("\u0194\u0194\u0194", Optional.of(new KerberosPrincipal("\u0194\u0194\u0194/example.com@EXAMPLE.COM")));
    private static final Identity kerberosInvalidAlice = new Identity("alice", Optional.of(new KerberosPrincipal("mallory/example.com@EXAMPLE.COM")));
    private static final Identity kerberosValidShare = new Identity("alice", Optional.of(new KerberosPrincipal("valid/example.com@EXAMPLE.COM")));
    private static final Identity kerberosInValidShare = new Identity("alice", Optional.of(new KerberosPrincipal("invalid/example.com@EXAMPLE.COM")));
    private static final Identity validSpecialRegexWildDot = new Identity(".*", Optional.of(new KerberosPrincipal("special/.*@EXAMPLE.COM")));
    private static final Identity validSpecialRegexEndQuote = new Identity("\\E", Optional.of(new KerberosPrincipal("special/\\E@EXAMPLE.COM")));
    private static final Identity invalidSpecialRegex = new Identity("alice", Optional.of(new KerberosPrincipal("special/.*@EXAMPLE.COM")));
    private static final Identity bob = new Identity("bob", Optional.empty());
    private static final Identity admin = new Identity("admin", Optional.empty());
    private static final Identity nonAsciiUser = new Identity("\u0194\u0194\u0194", Optional.empty());
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
