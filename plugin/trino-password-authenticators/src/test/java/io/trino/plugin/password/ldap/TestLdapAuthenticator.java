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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.password.Credential;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import org.testng.annotations.Test;

import javax.naming.NamingException;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestLdapAuthenticator
{
    private static final String BASE_DN = "base-dn";
    private static final String PATTERN_PREFIX = "pattern::";

    @Test
    public void testSingleBindPattern()
    {
        TestLdapAuthenticatorClient client = new TestLdapAuthenticatorClient();
        client.addCredentials("alice@example.com", "alice-pass");

        LdapAuthenticator ldapAuthenticator = new LdapAuthenticator(
                client,
                new LdapConfig()
                        .setUserBindSearchPatterns("${USER}@example.com"));

        assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "invalid"))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("unknown", "alice-pass"))
                .isInstanceOf(RuntimeException.class);
        assertEquals(ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"), new BasicPrincipal("alice"));
    }

    @Test
    public void testMultipleBindPattern()
    {
        TestLdapAuthenticatorClient client = new TestLdapAuthenticatorClient();

        LdapAuthenticator ldapAuthenticator = new LdapAuthenticator(
                client,
                new LdapConfig()
                        .setUserBindSearchPatterns("${USER}@example.com:${USER}@alt.example.com"));

        client.addCredentials("alice@example.com", "alice-pass");
        assertEquals(ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"), new BasicPrincipal("alice"));
        ldapAuthenticator.invalidateCache();

        client.addCredentials("bob@alt.example.com", "bob-pass");
        assertEquals(ldapAuthenticator.createAuthenticatedPrincipal("bob", "bob-pass"), new BasicPrincipal("bob"));
        ldapAuthenticator.invalidateCache();

        client.addCredentials("alice@alt.example.com", "alt-alice-pass");
        assertEquals(ldapAuthenticator.createAuthenticatedPrincipal("alice", "alt-alice-pass"), new BasicPrincipal("alice"));
        ldapAuthenticator.invalidateCache();
        assertEquals(ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"), new BasicPrincipal("alice"));
        ldapAuthenticator.invalidateCache();
    }

    @Test
    public void testGroupMembership()
    {
        TestLdapAuthenticatorClient client = new TestLdapAuthenticatorClient();
        client.addCredentials("alice@example.com", "alice-pass");

        LdapAuthenticator ldapAuthenticator = new LdapAuthenticator(
                client,
                new LdapConfig()
                        .setUserBindSearchPatterns("${USER}@example.com")
                        .setUserBaseDistinguishedName(BASE_DN)
                        .setGroupAuthorizationSearchPattern(PATTERN_PREFIX + "${USER}"));

        assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "invalid"))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("unknown", "alice-pass"))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"))
                .isInstanceOf(AccessDeniedException.class);
        client.addGroupMember("alice");
        assertEquals(ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"), new BasicPrincipal("alice"));
    }

    @Test
    public void testDistinguishedNameLookup()
    {
        TestLdapAuthenticatorClient client = new TestLdapAuthenticatorClient();
        client.addCredentials("alice@example.com", "alice-pass");

        LdapAuthenticator ldapAuthenticator = new LdapAuthenticator(
                client,
                new LdapConfig()
                        .setUserBaseDistinguishedName(BASE_DN)
                        .setGroupAuthorizationSearchPattern(PATTERN_PREFIX + "${USER}")
                        .setBindDistingushedName("server")
                        .setBindPassword("server-pass"));

        client.addCredentials("alice", "alice-pass");
        client.addCredentials("alice@example.com", "alice-pass");

        assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"))
                .isInstanceOf(RuntimeException.class);

        client.addCredentials("server", "server-pass");
        assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"))
                .isInstanceOf(RuntimeException.class);

        client.addDistinguishedNameForUser("alice", "bob@example.com");
        assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"))
                .isInstanceOf(RuntimeException.class);

        client.addCredentials("bob@example.com", "alice-pass");
        assertEquals(ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"), new BasicPrincipal("alice"));
        ldapAuthenticator.invalidateCache();

        client.addDistinguishedNameForUser("alice", "another-mapping");
        assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"))
                .isInstanceOf(AccessDeniedException.class);
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

    private static class TestLdapAuthenticatorClient
            implements LdapAuthenticatorClient
    {
        private final Set<Credential> credentials = new HashSet<>();
        private final Set<String> groupMembers = new HashSet<>();
        private final HashMultimap<String, String> userDNs = HashMultimap.create();

        public void addCredentials(String userDistinguishedName, String password)
        {
            credentials.add(new Credential(userDistinguishedName, password));
        }

        public void addGroupMember(String userName)
        {
            groupMembers.add(userName);
        }

        public void addDistinguishedNameForUser(String userName, String distinguishedName)
        {
            userDNs.put(userName, distinguishedName);
        }

        @Override
        public void validatePassword(String userDistinguishedName, String password)
                throws NamingException
        {
            if (!credentials.contains(new Credential(userDistinguishedName, password))) {
                throw new NamingException();
            }
        }

        @Override
        public boolean isGroupMember(String searchBase, String groupSearch, String contextUserDistinguishedName, String contextPassword)
                throws NamingException
        {
            validatePassword(contextUserDistinguishedName, contextPassword);
            return getSearchUser(searchBase, groupSearch)
                    .map(groupMembers::contains)
                    .orElse(false);
        }

        @Override
        public Set<String> lookupUserDistinguishedNames(String searchBase, String searchFilter, String contextUserDistinguishedName, String contextPassword)
                throws NamingException
        {
            validatePassword(contextUserDistinguishedName, contextPassword);
            return getSearchUser(searchBase, searchFilter)
                    .map(userDNs::get)
                    .orElse(ImmutableSet.of());
        }

        private static Optional<String> getSearchUser(String searchBase, String groupSearch)
        {
            if (!searchBase.equals(BASE_DN)) {
                return Optional.empty();
            }
            if (!groupSearch.startsWith(PATTERN_PREFIX)) {
                return Optional.empty();
            }
            return Optional.of(groupSearch.substring(PATTERN_PREFIX.length()));
        }
    }
}
