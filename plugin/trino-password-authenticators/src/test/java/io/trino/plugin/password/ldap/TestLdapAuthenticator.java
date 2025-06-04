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
package io.trino.plugin.password.ldap;

import com.google.common.io.Closer;
import io.trino.plugin.base.ldap.JdkLdapClient;
import io.trino.plugin.base.ldap.LdapClientConfig;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.testing.containers.TestingOpenLdapServer;
import io.trino.testing.containers.TestingOpenLdapServer.DisposableSubContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.containers.Network;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestLdapAuthenticator
{
    private final Closer closer;

    private final TestingOpenLdapServer openLdapServer;
    private final LdapAuthenticatorClient client;

    public TestLdapAuthenticator()
    {
        closer = Closer.create();
        Network network = Network.newNetwork();
        closer.register(network::close);

        openLdapServer = closer.register(new TestingOpenLdapServer(network));
        openLdapServer.start();

        client = new LdapAuthenticatorClient(
                new JdkLdapClient(new LdapClientConfig()
                        .setLdapUrl(openLdapServer.getLdapUrl())));
    }

    @AfterAll
    public void close()
            throws Exception
    {
        closer.close();
    }

    @Test
    public void testSingleBindPattern()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServer.createOrganization();
                DisposableSubContext ignored = openLdapServer.createUser(organization, "alice", "alice-pass")) {
            LdapAuthenticator ldapAuthenticator = new LdapAuthenticator(
                    client,
                    new LdapAuthenticatorConfig()
                            .setUserBindSearchPatterns("uid=${USER}," + organization.getDistinguishedName()));

            assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "invalid"))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageMatching("Access Denied: Invalid credentials");
            assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("unknown", "alice-pass"))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageMatching("Access Denied: Invalid credentials");
            assertThat(ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass")).isEqualTo(new BasicPrincipal("alice"));
        }
    }

    @Test
    public void testMultipleBindPattern()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServer.createOrganization();
                DisposableSubContext alternativeOrganization = openLdapServer.createOrganization();
                DisposableSubContext ignored = openLdapServer.createUser(organization, "alice", "alice-pass");
                DisposableSubContext ignored1 = openLdapServer.createUser(alternativeOrganization, "bob", "bob-pass");
                DisposableSubContext ignored2 = openLdapServer.createUser(alternativeOrganization, "alice", "alt-alice-pass")) {
            LdapAuthenticator ldapAuthenticator = new LdapAuthenticator(
                    client,
                    new LdapAuthenticatorConfig()
                            .setUserBindSearchPatterns(format("uid=${USER},%s:uid=${USER},%s", organization.getDistinguishedName(), alternativeOrganization.getDistinguishedName())));

            assertThat(ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass")).isEqualTo(new BasicPrincipal("alice"));
            ldapAuthenticator.invalidateCache();

            assertThat(ldapAuthenticator.createAuthenticatedPrincipal("bob", "bob-pass")).isEqualTo(new BasicPrincipal("bob"));
            ldapAuthenticator.invalidateCache();

            assertThat(ldapAuthenticator.createAuthenticatedPrincipal("alice", "alt-alice-pass")).isEqualTo(new BasicPrincipal("alice"));
            ldapAuthenticator.invalidateCache();
            assertThat(ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass")).isEqualTo(new BasicPrincipal("alice"));
            ldapAuthenticator.invalidateCache();
        }
    }

    @Test
    public void testGroupMembership()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServer.createOrganization();
                DisposableSubContext group = openLdapServer.createGroup(organization);
                DisposableSubContext alice = openLdapServer.createUser(organization, "alice", "alice-pass");
                DisposableSubContext ignored = openLdapServer.createUser(organization, "bob", "bob-pass")) {
            LdapAuthenticator ldapAuthenticator = new LdapAuthenticator(
                    client,
                    new LdapAuthenticatorConfig()
                            .setUserBindSearchPatterns("uid=${USER}," + organization.getDistinguishedName())
                            .setUserBaseDistinguishedName(organization.getDistinguishedName())
                            .setGroupAuthorizationSearchPattern(format("(&(objectClass=groupOfNames)(cn=group_*)(member=uid=${USER},%s))", organization.getDistinguishedName())));

            assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "invalid"))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageMatching("Access Denied: Invalid credentials");

            assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("unknown", "alice-pass"))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageMatching("Access Denied: Invalid credentials");

            assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("bob", "bob-pass"))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageMatching("Access Denied: User \\[bob] not a member of an authorized group");

            openLdapServer.addUserToGroup(alice, group);
            assertThat(ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass")).isEqualTo(new BasicPrincipal("alice"));
        }
    }

    @Test
    public void testInvalidBindPassword()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServer.createOrganization()) {
            LdapAuthenticator ldapAuthenticator = new LdapAuthenticator(
                    client,
                    new LdapAuthenticatorConfig()
                            .setUserBaseDistinguishedName(organization.getDistinguishedName())
                            .setGroupAuthorizationSearchPattern("(&(objectClass=inetOrgPerson))")
                            .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                            .setBindPassword("invalid-password"));

            assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageMatching("Access Denied: Invalid credentials");
        }
    }

    @Test
    public void testDistinguishedNameLookup()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServer.createOrganization();
                DisposableSubContext group = openLdapServer.createGroup(organization);
                DisposableSubContext alice = openLdapServer.createUser(organization, "alice", "alice-pass");
                DisposableSubContext bob = openLdapServer.createUser(organization, "bob", "bob-pass")) {
            LdapAuthenticator ldapAuthenticator = new LdapAuthenticator(
                    client,
                    new LdapAuthenticatorConfig()
                            .setUserBaseDistinguishedName(organization.getDistinguishedName())
                            .setGroupAuthorizationSearchPattern(format("(&(objectClass=inetOrgPerson)(memberof=%s))", group.getDistinguishedName()))
                            .setBindDistinguishedName("cn=admin,dc=trino,dc=testldap,dc=com")
                            .setBindPassword("admin"));

            assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("unknown_user", "invalid"))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageMatching("Access Denied: User \\[unknown_user] not a member of an authorized group");

            assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "invalid"))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageMatching("Access Denied: User \\[alice] not a member of an authorized group");
            ldapAuthenticator.invalidateCache();

            assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageMatching("Access Denied: User \\[alice] not a member of an authorized group");
            ldapAuthenticator.invalidateCache();

            assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("bob", "bob-pass"))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageMatching("Access Denied: User \\[bob] not a member of an authorized group");
            ldapAuthenticator.invalidateCache();

            openLdapServer.addUserToGroup(alice, group);
            assertThat(ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass")).isEqualTo(new BasicPrincipal("alice"));
            ldapAuthenticator.invalidateCache();

            assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "invalid"))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageMatching("Access Denied: Invalid credentials");
            ldapAuthenticator.invalidateCache();

            // Now group authorization filter will return multiple entries
            openLdapServer.addUserToGroup(bob, group);
            assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"))
                    .isInstanceOf(AccessDeniedException.class)
                    .hasMessageMatching("Access Denied: Multiple group membership results for user \\[alice].*");
            ldapAuthenticator.invalidateCache();
        }
    }

    @Test
    public void testContainsSpecialCharacters()
    {
        assertThat(LdapAuthenticator.containsSpecialCharacters("The quick brown fox jumped over the lazy dogs"))
                .as("English pangram")
                .isEqualTo(false);
        assertThat(LdapAuthenticator.containsSpecialCharacters("Pchnąć w tę łódź jeża lub ośm skrzyń fig"))
                .as("Perfect polish pangram")
                .isEqualTo(false);
        assertThat(LdapAuthenticator.containsSpecialCharacters("いろはにほへと ちりぬるを わかよたれそ つねならむ うゐのおくやま けふこえて あさきゆめみし ゑひもせす（ん）"))
                .as("Japanese hiragana pangram - Iroha")
                .isEqualTo(false);
        assertThat(LdapAuthenticator.containsSpecialCharacters("*"))
                .as("LDAP wildcard")
                .isEqualTo(true);
        assertThat(LdapAuthenticator.containsSpecialCharacters("   John Doe"))
                .as("Beginning with whitespace")
                .isEqualTo(true);
        assertThat(LdapAuthenticator.containsSpecialCharacters("John Doe  \r"))
                .as("Ending with whitespace")
                .isEqualTo(true);
        assertThat(LdapAuthenticator.containsSpecialCharacters("Hi (This) = is * a \\ test # ç à ô"))
                .as("Multiple special characters")
                .isEqualTo(true);
        assertThat(LdapAuthenticator.containsSpecialCharacters("John\u0000Doe"))
                .as("NULL character")
                .isEqualTo(true);
        assertThat(LdapAuthenticator.containsSpecialCharacters("John Doe <john.doe@company.com>"))
                .as("Angle brackets")
                .isEqualTo(true);
    }
}
