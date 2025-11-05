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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestLdapGroupProviderIntegration
{
    private final LdapGroupProviderFactory factory = new LdapGroupProviderFactory();

    private static final List<ConfigBuilder> CONFIG_BUILDERS;

    static {
        ConfigBuilder withMemberOf = builder -> {
            builder.put("ldap.user-member-of-attribute", "memberOf");
            return builder;
        };

        ConfigBuilder withGroupFilter = builder -> {
            builder.put("ldap.use-group-filter", "true");
            builder.put("ldap.group-base-dn", "ou=groups,dc=trino,dc=testldap,dc=com");
            return builder;
        };

        CONFIG_BUILDERS = ImmutableList.of(withMemberOf, withGroupFilter);
    }

    private Closer closer;
    private Map<String, String> baseConfig;
    private DisposableSubContext clients;
    private DisposableSubContext developers;
    private DisposableSubContext qualityAssurance;

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
        for (ConfigBuilder configBuilder : CONFIG_BUILDERS) {
            assertGetGroups(configBuilder, "alicea", ImmutableSet.of("clients", "developers", "qualityAssurance"));
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

        assertThat(groups).containsAll(expectedGroups);
    }

    @Test
    public void testGetGroupsWithGroupsFilter()
    {
        assertGetGroupsWithGroupsFilter("alicea", "cn=*", ImmutableSet.of("clients", "developers", "qualityAssurance"));
        assertGetGroupsWithGroupsFilter("alicea", "cn=dev*", ImmutableSet.of("developers"));
        assertGetGroupsWithGroupsFilter("alicea", "(|(cn=dev*)(cn=cl*))", ImmutableSet.of("developers", "clients"));
        assertGetGroupsWithGroupsFilter("alicea", "(&(objectclass=groupOfNames)(!(ou:dn:=external)))", ImmutableSet.of("developers", "qualityAssurance"));
    }

    private void assertGetGroupsWithGroupsFilter(String userName, String groupFilter, Set<String> expectedGroups)
    {
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .putAll(baseConfig)
                .put("ldap.use-group-filter", "true")
                .put("ldap.group-search-member-attribute", "member")
                .put("ldap.group-name-attribute", "cn")
                .put("ldap.group-base-dn", "ou=groups,dc=trino,dc=testldap,dc=com")
                .put("ldap.group-search-filter", groupFilter)
                .buildOrThrow();
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups(userName);

        assertThat(groups).containsAll(expectedGroups);
    }

    @Test
    public void testGetGroupForMissingUserReturnsEmpty()
    {
        for (ConfigBuilder configBuilder : CONFIG_BUILDERS) {
            assertGetGroupForMissingUserReturnsEmpty(configBuilder);
            assertGetGroupForMissingUserReturnsEmpty(configBuilder);
            assertGetGroupForMissingUserReturnsEmpty(configBuilder);
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
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .putAll(baseConfig)
                .put("ldap.use-group-filter", "true")
                .put("ldap.group-search-member-attribute", "some-attribute-that-does-not-exist")
                .put("ldap.group-name-attribute", "cn")
                .put("ldap.group-base-dn", "ou=groups,dc=trino,dc=testldap,dc=com")
                .buildOrThrow();
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups("alicea");

        assertThat(groups).isEmpty();
    }

    @Test
    public void testGetGroupsWithBadGroupNameReturnsFullName()
    {
        for (ConfigBuilder configBuilder : CONFIG_BUILDERS) {
            assertGetGroupsWithBadGroupNameReturnsFullName(configBuilder);
            assertGetGroupsWithBadGroupNameReturnsFullName(configBuilder);
            assertGetGroupsWithBadGroupNameReturnsFullName(configBuilder);
            assertGetGroupsWithBadGroupNameReturnsFullName(configBuilder);
        }
    }

    private void assertGetGroupsWithBadGroupNameReturnsFullName(ConfigBuilder configBuilder)
    {
        Map<String, String> config = configBuilder.apply(new HashMap<>(baseConfig));
        config.put("ldap.group-name-attribute", "some-attribute-that-does-not-exist");
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups("alicea");

        assertThat(groups).containsAll(ImmutableSet.of(clients.getDistinguishedName(), developers.getDistinguishedName(), qualityAssurance.getDistinguishedName()));
    }

    @Test
    public void testGetGroupsConcurrently()
            throws InterruptedException
    {
        for (ConfigBuilder configBuilder : CONFIG_BUILDERS) {
            assertGetGroupsConcurrently(configBuilder);
            assertGetGroupsConcurrently(configBuilder);
            assertGetGroupsConcurrently(configBuilder);
            assertGetGroupsConcurrently(configBuilder);
        }
    }

    private void assertGetGroupsConcurrently(ConfigBuilder configBuilder)
            throws InterruptedException
    {
        Map<String, String> config = configBuilder.apply(new HashMap<>(baseConfig));
        GroupProvider groupsProvider = factory.create(config);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(4);

        CompletableFuture.supplyAsync(() -> groupsProvider.getGroups("alicea"), executor).whenComplete((g, t) -> {
            assertThat(g).containsAll(ImmutableSet.of("clients", "qualityAssurance", "developers"));
            latch.countDown();
        });

        CompletableFuture.supplyAsync(() -> groupsProvider.getGroups("bobq"), executor).whenComplete((g, t) -> {
            assertThat(g).containsAll(ImmutableSet.of("qualityAssurance"));
            latch.countDown();
        });

        CompletableFuture.supplyAsync(() -> groupsProvider.getGroups("johnb"), executor).whenComplete((g, t) -> {
            assertThat(g).containsAll(ImmutableSet.of("clients"));
            latch.countDown();
        });

        CompletableFuture.supplyAsync(() -> groupsProvider.getGroups("carlp"), executor).whenComplete((g, t) -> {
            assertThat(g).isEmpty();
            latch.countDown();
        });

        latch.await();
    }

    @FunctionalInterface
    public interface ConfigBuilder
    {
        Map<String, String> apply(Map<String, String> builder);
    }
}
