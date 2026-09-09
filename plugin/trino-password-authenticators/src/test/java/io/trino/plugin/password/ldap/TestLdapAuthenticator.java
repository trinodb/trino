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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.trino.plugin.base.ldap.JdkLdapClient;
import io.trino.plugin.base.ldap.LdapClientConfig;
import io.trino.plugin.base.ldap.LdapQuery;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.testing.containers.TestingOpenLdapServer;
import io.trino.testing.containers.TestingOpenLdapServer.DisposableSubContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.containers.Network;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.Control;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.PagedResultsControl;
import javax.naming.ldap.PagedResultsResponseControl;

import java.util.List;
import java.util.Properties;
import java.util.Set;

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
    public void testUserDistinguishedNameLookupStopsAtSecondMatch()
            throws Exception
    {
        // User DN lookup only needs to tell apart zero, one and multiple matches. A broad filter matching five
        // users (spread across pages of 2) must be short-circuited after the second DN rather than draining
        // every page, so the caller can still detect the "multiple results" case cheaply.
        LdapAuthenticatorClient pagedClient = new LdapAuthenticatorClient(
                new JdkLdapClient(new LdapClientConfig()
                        .setLdapUrl(openLdapServer.getLdapUrl())
                        .setLdapPagingSize(2)));

        try (DisposableSubContext organization = openLdapServer.createOrganization();
                DisposableSubContext ignored0 = openLdapServer.createUser(organization, "paged_user_0", "pass-0");
                DisposableSubContext ignored1 = openLdapServer.createUser(organization, "paged_user_1", "pass-1");
                DisposableSubContext ignored2 = openLdapServer.createUser(organization, "paged_user_2", "pass-2");
                DisposableSubContext ignored3 = openLdapServer.createUser(organization, "paged_user_3", "pass-3");
                DisposableSubContext ignored4 = openLdapServer.createUser(organization, "paged_user_4", "pass-4")) {
            Set<String> distinguishedNames = pagedClient.lookupUserDistinguishedNames(
                    organization.getDistinguishedName(),
                    "(objectClass=inetOrgPerson)",
                    "cn=admin,dc=trino,dc=testldap,dc=com",
                    "admin");

            assertThat(distinguishedNames)
                    .as("user DN lookup must stop after the second match")
                    .hasSize(2);
        }
    }

    @Test
    public void testPagedLookupReadsEveryPage()
            throws Exception
    {
        // The full paged enumeration (as used by the group providers) must return every matching entry across
        // all pages, regardless of page size. Active Directory truncates a non-paged search at its MaxPageSize.
        try (DisposableSubContext organization = openLdapServer.createOrganization();
                DisposableSubContext ignored0 = openLdapServer.createUser(organization, "paged_user_0", "pass-0");
                DisposableSubContext ignored1 = openLdapServer.createUser(organization, "paged_user_1", "pass-1");
                DisposableSubContext ignored2 = openLdapServer.createUser(organization, "paged_user_2", "pass-2");
                DisposableSubContext ignored3 = openLdapServer.createUser(organization, "paged_user_3", "pass-3");
                DisposableSubContext ignored4 = openLdapServer.createUser(organization, "paged_user_4", "pass-4")) {
            Set<String> expectedDistinguishedNames = ImmutableSet.of(
                    format("uid=paged_user_0,%s", organization.getDistinguishedName()),
                    format("uid=paged_user_1,%s", organization.getDistinguishedName()),
                    format("uid=paged_user_2,%s", organization.getDistinguishedName()),
                    format("uid=paged_user_3,%s", organization.getDistinguishedName()),
                    format("uid=paged_user_4,%s", organization.getDistinguishedName()));

            for (int pageSize : new int[] {1, 2, 3, 10}) {
                assertThat(readAllDistinguishedNames(pageSize, organization.getDistinguishedName(), "(objectClass=inetOrgPerson)"))
                        .as("page size %s must return exactly the five matching users across all pages", pageSize)
                        .containsExactlyInAnyOrderElementsOf(expectedDistinguishedNames);
            }
        }
    }

    private Set<String> readAllDistinguishedNames(int pageSize, String searchBase, String filter)
            throws NamingException
    {
        JdkLdapClient client = new JdkLdapClient(new LdapClientConfig()
                .setLdapUrl(openLdapServer.getLdapUrl())
                .setLdapPagingSize(pageSize));
        return client.executeLdapQuery(
                "cn=admin,dc=trino,dc=testldap,dc=com",
                "admin",
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(searchBase)
                        .withSearchFilter(filter)
                        .build(),
                searchResults -> {
                    ImmutableSet.Builder<String> distinguishedNames = ImmutableSet.builder();
                    while (searchResults.hasMore()) {
                        distinguishedNames.add(searchResults.next().getNameInNamespace());
                    }
                    return distinguishedNames.build();
                });
    }

    @Test
    public void testPagedSearchSplitsResultsIntoExpectedPages()
            throws Exception
    {
        try (DisposableSubContext organization = openLdapServer.createOrganization();
                DisposableSubContext ignored0 = openLdapServer.createUser(organization, "paged_user_0", "pass-0");
                DisposableSubContext ignored1 = openLdapServer.createUser(organization, "paged_user_1", "pass-1");
                DisposableSubContext ignored2 = openLdapServer.createUser(organization, "paged_user_2", "pass-2");
                DisposableSubContext ignored3 = openLdapServer.createUser(organization, "paged_user_3", "pass-3");
                DisposableSubContext ignored4 = openLdapServer.createUser(organization, "paged_user_4", "pass-4")) {
            assertThat(pageSizes(organization.getDistinguishedName(), "(objectClass=inetOrgPerson)", 2))
                    .as("five users with page size 2 must arrive as pages of 2, 2 and 1")
                    .containsExactly(2, 2, 1);
        }
    }

    private List<Integer> pageSizes(String searchBase, String filter, int pageSize)
            throws Exception
    {
        Properties environment = new Properties();
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        environment.put(Context.PROVIDER_URL, openLdapServer.getLdapUrl());
        environment.put(Context.SECURITY_AUTHENTICATION, "simple");
        environment.put(Context.SECURITY_PRINCIPAL, "cn=admin,dc=trino,dc=testldap,dc=com");
        environment.put(Context.SECURITY_CREDENTIALS, "admin");

        LdapContext context = new InitialLdapContext(environment, null);
        try {
            SearchControls searchControls = new SearchControls();
            searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

            ImmutableList.Builder<Integer> pages = ImmutableList.builder();
            byte[] cookie = null;
            do {
                context.setRequestControls(new Control[] {new PagedResultsControl(pageSize, cookie, Control.NONCRITICAL)});
                NamingEnumeration<SearchResult> page = context.search(searchBase, filter, searchControls);
                int count = 0;
                while (page.hasMore()) {
                    page.next();
                    count++;
                }
                pages.add(count);
                cookie = findCookie(context.getResponseControls());
            }
            while (cookie != null);
            return pages.build();
        }
        finally {
            context.close();
        }
    }

    private static byte[] findCookie(Control[] controls)
    {
        if (controls == null) {
            return null;
        }
        for (Control control : controls) {
            if (control instanceof PagedResultsResponseControl pagedResultsResponseControl) {
                byte[] cookie = pagedResultsResponseControl.getCookie();
                if (cookie != null && cookie.length > 0) {
                    return cookie;
                }
            }
        }
        return null;
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
