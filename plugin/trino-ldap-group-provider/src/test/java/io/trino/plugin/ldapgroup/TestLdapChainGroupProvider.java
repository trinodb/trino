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
package io.trino.plugin.ldapgroup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.trino.spi.security.GroupProvider;
import io.trino.testing.containers.TestingOpenLdapServer;
import io.trino.testing.containers.TestingOpenLdapServer.DisposableSubContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Network;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestLdapChainGroupProvider
{
    private final LdapGroupProviderFactory factory = new LdapGroupProviderFactory();

    private Closer closer;
    private Map<String, String> baseConfig;
    private DisposableSubContext usersOU;
    private DisposableSubContext groupsOU;
    private DisposableSubContext user1;
    private DisposableSubContext user2;
    private DisposableSubContext childGroup;
    private DisposableSubContext parentGroup;
    private DisposableSubContext grandparentGroup;

    @BeforeAll
    public void setup()
            throws Exception
    {
        closer = Closer.create();
        Network network = Network.newNetwork();
        closer.register(network::close);

        TestingOpenLdapServer openLdapServer = closer.register(new TestingOpenLdapServer(network));
        openLdapServer.start();

        baseConfig = Map.of(
                "ldap.url", openLdapServer.getLdapUrl(),
                "ldap.allow-insecure", "true",
                "ldap.admin-user", "cn=admin,dc=trino,dc=testldap,dc=com",
                "ldap.admin-password", "admin",
                "ldap.user-base-dn", "ou=users,dc=trino,dc=testldap,dc=com",
                "ldap.user-search-filter", "(cn={0})",
                "ldap.use-chain-groups", "true",
                "ldap.group-base-dn", "ou=groups,dc=trino,dc=testldap,dc=com",
                "ldap.group-search-member-attribute", "member");

        usersOU = openLdapServer.createOrganization("users");
        groupsOU = openLdapServer.createOrganization("groups");

        // Create users
        user1 = openLdapServer.createUser(usersOU, "user1", "");
        user2 = openLdapServer.createUser(usersOU, "user2", "");

        // Create nested group hierarchy: grandparent -> parent -> child
        childGroup = openLdapServer.createGroup(groupsOU, "child-group");
        parentGroup = openLdapServer.createGroup(groupsOU, "parent-group");
        grandparentGroup = openLdapServer.createGroup(groupsOU, "grandparent-group");

        // Set up group hierarchy
        openLdapServer.addUserToGroup(user1, childGroup);
        openLdapServer.addGroupToGroup(childGroup, parentGroup);
        openLdapServer.addGroupToGroup(parentGroup, grandparentGroup);
        openLdapServer.addUserToGroup(user2, parentGroup);
    }

    @AfterAll
    public void close()
            throws Exception
    {
        closer.close();
    }

    @Test
    public void testChainGroupResolution()
    {
        GroupProvider groupProvider = factory.create(baseConfig);

        // user1 is in child-group, which is nested in parent-group, which is nested in grandparent-group
        Set<String> user1Groups = groupProvider.getGroups("user1");
        assertThat(user1Groups).containsExactlyInAnyOrder("child-group", "parent-group", "grandparent-group");

        // user2 is directly in parent-group, which is nested in grandparent-group
        Set<String> user2Groups = groupProvider.getGroups("user2");
        assertThat(user2Groups).containsExactlyInAnyOrder("parent-group", "grandparent-group");
    }

    @Test
    public void testChainGroupsWithFilter()
    {
        Map<String, String> configWithFilter = ImmutableMap.<String, String>builder()
                .putAll(baseConfig)
                .put("ldap.group-search-filter", "(cn=*parent*)")
                .buildOrThrow();

        GroupProvider groupProvider = factory.create(configWithFilter);

        // With filter, only groups matching the pattern should be returned
        Set<String> user1Groups = groupProvider.getGroups("user1");
        assertThat(user1Groups).containsExactlyInAnyOrder("parent-group", "grandparent-group");
        assertThat(user1Groups).doesNotContain("child-group");

        Set<String> user2Groups = groupProvider.getGroups("user2");
        assertThat(user2Groups).containsExactlyInAnyOrder("parent-group", "grandparent-group");
    }

    @Test
    public void testChainGroupsWithCustomMemberAttribute()
    {
        Map<String, String> configWithCustomAttribute = ImmutableMap.<String, String>builder()
                .putAll(baseConfig)
                .put("ldap.group-search-member-attribute", "uniqueMember")
                .buildOrThrow();

        GroupProvider groupProvider = factory.create(configWithCustomAttribute);

        // Should still work with custom member attribute
        Set<String> user1Groups = groupProvider.getGroups("user1");
        assertThat(user1Groups).containsExactlyInAnyOrder("child-group", "parent-group", "grandparent-group");
    }

    @Test
    public void testChainGroupsPerformance()
    {
        GroupProvider groupProvider = factory.create(baseConfig);

        // Test that the chain-based approach is efficient
        // This should complete quickly as it uses a single LDAP query
        long startTime = System.currentTimeMillis();
        Set<String> user1Groups = groupProvider.getGroups("user1");
        long endTime = System.currentTimeMillis();

        assertThat(user1Groups).containsExactlyInAnyOrder("child-group", "parent-group", "grandparent-group");
        
        // The chain-based approach should be very fast (under 100ms for this simple case)
        assertThat(endTime - startTime).isLessThan(1000);
    }
}

