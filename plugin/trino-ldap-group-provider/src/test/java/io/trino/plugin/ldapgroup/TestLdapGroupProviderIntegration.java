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

import com.google.common.collect.ImmutableList;
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
import org.junit.jupiter.api.parallel.Execution;
import org.testcontainers.containers.Network;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.trino.plugin.ldapgroup.LdapFilteringGroupProviderConfig.LdapGroupSearchMode.DIRECT;
import static io.trino.plugin.ldapgroup.LdapFilteringGroupProviderConfig.LdapGroupSearchMode.RECURSIVE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestLdapGroupProviderIntegration
{
    private final LdapGroupProviderFactory factory = new LdapGroupProviderFactory();

    private static final ConfigBuilder WITH_MEMBER_OF = builder -> {
        builder.put("ldap.user-member-of-attribute", "memberOf");
        return builder;
    };

    private static final ConfigBuilder WITH_GROUP_FILTER = builder -> {
        builder.put("ldap.use-group-filter", "true");
        builder.put("ldap.group-base-dn", "ou=groups,dc=trino,dc=testldap,dc=com");
        return builder;
    };

    private static final ConfigBuilder WITH_GROUP_FILTER_NESTED = builder -> {
        builder.put("ldap.use-group-filter", "true");
        builder.put("ldap.group-base-dn", "ou=groups,dc=trino,dc=testldap,dc=com");
        builder.put("ldap.group-search-mode", "RECURSIVE");
        return builder;
    };

    private static final List<ConfigBuilder> CONFIG_BUILDERS = ImmutableList.of(WITH_MEMBER_OF, WITH_GROUP_FILTER, WITH_GROUP_FILTER_NESTED);

    private Closer closer;
    private Map<String, String> baseConfig;
    private DisposableSubContext clients;
    private DisposableSubContext developers;
    private DisposableSubContext qualityAssurance;
    private DisposableSubContext engineering;
    private DisposableSubContext division;

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
                "ldap.user-search-filter", "(cn={0})");

        DisposableSubContext groupsOU = openLdapServer.createOrganization("groups");
        DisposableSubContext externalGroupsOU = openLdapServer.createOrganization("external", groupsOU);
        DisposableSubContext usersOU = openLdapServer.createOrganization("users");

        DisposableSubContext johnb = openLdapServer.createUser(usersOU, "johnb", "");
        DisposableSubContext alicea = openLdapServer.createUser(usersOU, "alicea", "");
        DisposableSubContext bobq = openLdapServer.createUser(usersOU, "bobq", "");
        openLdapServer.createUser(usersOU, "carlp", "");

        clients = openLdapServer.createGroup(externalGroupsOU, "clients");
        openLdapServer.addUserToGroup(johnb, clients);
        openLdapServer.addUserToGroup(alicea, clients);

        developers = openLdapServer.createGroup(groupsOU, "developers");
        openLdapServer.addUserToGroup(alicea, developers);

        qualityAssurance = openLdapServer.createGroup(groupsOU, "qualityAssurance");
        openLdapServer.addUserToGroup(alicea, qualityAssurance);
        openLdapServer.addUserToGroup(bobq, qualityAssurance);

        engineering = openLdapServer.createGroup(groupsOU, "engineering");
        // Nest in both directions so RECURSIVE mode has a cycle to terminate on
        openLdapServer.addUserToGroup(developers, engineering);
        openLdapServer.addUserToGroup(engineering, developers);

        division = openLdapServer.createGroup(groupsOU, "division");
        openLdapServer.addUserToGroup(engineering, division);
    }

    @AfterAll
    public void close()
            throws Exception
    {
        closer.close();
    }

    @Test
    public void testGetGroups()
    {
        assertGetGroups(WITH_MEMBER_OF, "alicea", ImmutableSet.of("clients", "developers", "qualityAssurance"));
        assertGetGroups(WITH_GROUP_FILTER, "alicea", ImmutableSet.of("clients", "developers", "qualityAssurance"));
        assertGetGroups(WITH_GROUP_FILTER_NESTED, "alicea", ImmutableSet.of("clients", "developers", "qualityAssurance", "engineering", "division"));

        for (ConfigBuilder configBuilder : CONFIG_BUILDERS) {
            assertGetGroups(configBuilder, "johnb", ImmutableSet.of("clients"));
            assertGetGroups(configBuilder, "bobq", ImmutableSet.of("qualityAssurance"));
            assertGetGroups(configBuilder, "carlp", ImmutableSet.of());
        }
    }

    private void assertGetGroups(ConfigBuilder configBuilder, String userName, Set<String> expectedGroups)
    {
        Map<String, String> config = configBuilder.apply(new HashMap<>(baseConfig));
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups(userName);

        assertThat(groups).isEqualTo(expectedGroups);
    }

    @Test
    public void testGetGroupsWithGroupsFilterDirect()
    {
        assertGetGroupsWithGroupsFilter(DIRECT, "alicea", "cn=*", ImmutableSet.of("clients", "developers", "qualityAssurance"));
        assertGetGroupsWithGroupsFilter(DIRECT, "alicea", "cn=dev*", ImmutableSet.of("developers"));
        assertGetGroupsWithGroupsFilter(DIRECT, "alicea", "cn=eng*", ImmutableSet.of());
        assertGetGroupsWithGroupsFilter(DIRECT, "alicea", "(|(cn=dev*)(cn=cl*))", ImmutableSet.of("developers", "clients"));
        assertGetGroupsWithGroupsFilter(DIRECT, "alicea", "(&(objectclass=groupOfNames)(!(ou:dn:=external)))", ImmutableSet.of("developers", "qualityAssurance"));
        assertGetGroupsWithGroupsFilter(DIRECT, "alicea", "cn=does-not-match", ImmutableSet.of());
    }

    @Test
    public void testGetGroupsWithGroupsFilterRecursive()
    {
        assertGetGroupsWithGroupsFilter(RECURSIVE, "alicea", "cn=*", ImmutableSet.of("clients", "developers", "qualityAssurance", "engineering", "division"));
        assertGetGroupsWithGroupsFilter(RECURSIVE, "alicea", "cn=dev*", ImmutableSet.of("developers"));
        assertGetGroupsWithGroupsFilter(RECURSIVE, "alicea", "cn=eng*", ImmutableSet.of("engineering"));
        assertGetGroupsWithGroupsFilter(RECURSIVE, "alicea", "cn=division", ImmutableSet.of("division"));
        assertGetGroupsWithGroupsFilter(RECURSIVE, "alicea", "(|(cn=dev*)(cn=cl*))", ImmutableSet.of("developers", "clients"));
        assertGetGroupsWithGroupsFilter(RECURSIVE, "alicea", "(&(objectclass=groupOfNames)(!(ou:dn:=external)))", ImmutableSet.of("developers", "qualityAssurance", "engineering", "division"));
        assertGetGroupsWithGroupsFilter(RECURSIVE, "alicea", "cn=does-not-match", ImmutableSet.of());
    }

    private void assertGetGroupsWithGroupsFilter(LdapFilteringGroupProviderConfig.LdapGroupSearchMode searchMode, String userName, String groupFilter, Set<String> expectedGroups)
    {
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .putAll(baseConfig)
                .put("ldap.use-group-filter", "true")
                .put("ldap.group-search-member-attribute", "member")
                .put("ldap.group-name-attribute", "cn")
                .put("ldap.group-base-dn", "ou=groups,dc=trino,dc=testldap,dc=com")
                .put("ldap.group-search-mode", searchMode.name())
                .put("ldap.group-search-filter", groupFilter)
                .buildOrThrow();
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups(userName);

        assertThat(groups).isEqualTo(expectedGroups);
    }

    @Test
    public void testGetGroupForMissingUserReturnsEmpty()
    {
        for (ConfigBuilder configBuilder : CONFIG_BUILDERS) {
            assertGetGroupForMissingUserReturnsEmpty(configBuilder);
        }
    }

    private void assertGetGroupForMissingUserReturnsEmpty(ConfigBuilder configBuilder)
    {
        Map<String, String> config = configBuilder.apply(new HashMap<>(baseConfig));
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups("wrong-user-name");

        assertThat(groups).isEmpty();
    }

    @Test
    public void testGetGroupsWithBadGroupMemberAttributeReturnsEmpty()
    {
        for (LdapFilteringGroupProviderConfig.LdapGroupSearchMode searchMode : ImmutableList.of(DIRECT, RECURSIVE)) {
            Map<String, String> config = ImmutableMap.<String, String>builder()
                    .putAll(baseConfig)
                    .put("ldap.use-group-filter", "true")
                    .put("ldap.group-search-member-attribute", "some-attribute-that-does-not-exist")
                    .put("ldap.group-name-attribute", "cn")
                    .put("ldap.group-base-dn", "ou=groups,dc=trino,dc=testldap,dc=com")
                    .put("ldap.group-search-mode", searchMode.name())
                    .buildOrThrow();
            GroupProvider groupsProvider = factory.create(config);

            Set<String> groups = groupsProvider.getGroups("alicea");

            assertThat(groups).isEmpty();
        }
    }

    @Test
    public void testGetGroupsWithBadGroupNameReturnsFullName()
    {
        assertGetGroupsWithBadGroupNameReturnsFullName(WITH_MEMBER_OF, ImmutableSet.of(clients.getDistinguishedName(), developers.getDistinguishedName(), qualityAssurance.getDistinguishedName()));
        assertGetGroupsWithBadGroupNameReturnsFullName(WITH_GROUP_FILTER, ImmutableSet.of(clients.getDistinguishedName(), developers.getDistinguishedName(), qualityAssurance.getDistinguishedName()));
        assertGetGroupsWithBadGroupNameReturnsFullName(WITH_GROUP_FILTER_NESTED, ImmutableSet.of(clients.getDistinguishedName(), developers.getDistinguishedName(), qualityAssurance.getDistinguishedName(), engineering.getDistinguishedName(), division.getDistinguishedName()));
    }

    private void assertGetGroupsWithBadGroupNameReturnsFullName(ConfigBuilder configBuilder, Set<String> expectedGroups)
    {
        Map<String, String> config = configBuilder.apply(new HashMap<>(baseConfig));
        config.put("ldap.group-name-attribute", "some-attribute-that-does-not-exist");
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups("alicea");

        assertThat(groups).isEqualTo(expectedGroups);
    }

    @Test
    public void testGetGroupsConcurrently()
    {
        assertGetGroupsConcurrently(WITH_MEMBER_OF, ImmutableSet.of("clients", "qualityAssurance", "developers"));
        assertGetGroupsConcurrently(WITH_GROUP_FILTER, ImmutableSet.of("clients", "qualityAssurance", "developers"));
        assertGetGroupsConcurrently(WITH_GROUP_FILTER_NESTED, ImmutableSet.of("clients", "qualityAssurance", "developers", "engineering", "division"));
    }

    private void assertGetGroupsConcurrently(ConfigBuilder configBuilder, Set<String> expectedAliceGroups)
    {
        Map<String, String> config = configBuilder.apply(new HashMap<>(baseConfig));
        GroupProvider groupsProvider = factory.create(config);

        try (ExecutorService executor = Executors.newFixedThreadPool(4)) {
            CompletableFuture<Void> alice = CompletableFuture.supplyAsync(() -> groupsProvider.getGroups("alicea"), executor)
                    .thenAccept(groups -> assertThat(groups).isEqualTo(expectedAliceGroups));
            CompletableFuture<Void> bob = CompletableFuture.supplyAsync(() -> groupsProvider.getGroups("bobq"), executor)
                    .thenAccept(groups -> assertThat(groups).isEqualTo(ImmutableSet.of("qualityAssurance")));
            CompletableFuture<Void> john = CompletableFuture.supplyAsync(() -> groupsProvider.getGroups("johnb"), executor)
                    .thenAccept(groups -> assertThat(groups).isEqualTo(ImmutableSet.of("clients")));
            CompletableFuture<Void> carl = CompletableFuture.supplyAsync(() -> groupsProvider.getGroups("carlp"), executor)
                    .thenAccept(groups -> assertThat(groups).isEmpty());

            CompletableFuture.allOf(alice, bob, john, carl).join();
        }
    }

    @FunctionalInterface
    public interface ConfigBuilder
    {
        Map<String, String> apply(Map<String, String> builder);
    }
}
