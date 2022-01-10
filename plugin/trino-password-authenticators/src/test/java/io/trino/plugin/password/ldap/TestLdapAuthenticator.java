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

import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestLdapAuthenticator
{
    @Test
    public void testSingleBindPattern()
    {
        TestLdapClient client = new TestLdapClient();
        client.addCredentials("alice@example.com", "alice-pass");
        LdapConfig ldapConfig = new LdapConfig()
                .setUserBindSearchPatterns("${USER}@example.com");
        LdapCommon ldapCommon = new LdapCommon(client, ldapConfig);

        LdapAuthenticator ldapAuthenticator = new LdapAuthenticator(client, ldapConfig, ldapCommon);

        assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("alice", "invalid"))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> ldapAuthenticator.createAuthenticatedPrincipal("unknown", "alice-pass"))
                .isInstanceOf(RuntimeException.class);
        assertEquals(ldapAuthenticator.createAuthenticatedPrincipal("alice", "alice-pass"), new BasicPrincipal("alice"));
    }

    @Test
    public void testMultipleBindPattern()
    {
        TestLdapClient client = new TestLdapClient();
        LdapConfig ldapConfig = new LdapConfig()
                .setUserBindSearchPatterns("${USER}@example.com:${USER}@alt.example.com");
        LdapCommon ldapCommon = new LdapCommon(client, ldapConfig);

        LdapAuthenticator ldapAuthenticator = new LdapAuthenticator(client, ldapConfig, ldapCommon);

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
        TestLdapClient client = new TestLdapClient();
        client.addCredentials("alice@example.com", "alice-pass");
        LdapConfig ldapConfig = new LdapConfig()
                .setUserBindSearchPatterns("${USER}@example.com")
                .setUserBaseDistinguishedName(TestLdapClient.BASE_DN)
                .setGroupAuthorizationSearchPattern(TestLdapClient.PATTERN_PREFIX + "${USER}");
        LdapCommon ldapCommon = new LdapCommon(client, ldapConfig);

        LdapAuthenticator ldapAuthenticator = new LdapAuthenticator(client, ldapConfig, ldapCommon);

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
        TestLdapClient client = new TestLdapClient();
        client.addCredentials("alice@example.com", "alice-pass");
        LdapConfig ldapConfig = new LdapConfig()
                .setUserBaseDistinguishedName(TestLdapClient.BASE_DN)
                .setGroupAuthorizationSearchPattern(TestLdapClient.PATTERN_PREFIX + "${USER}")
                .setBindDistingushedName("server")
                .setBindPassword("server-pass");
        LdapCommon ldapCommon = new LdapCommon(client, ldapConfig);

        LdapAuthenticator ldapAuthenticator = new LdapAuthenticator(client, ldapConfig, ldapCommon);

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
}
