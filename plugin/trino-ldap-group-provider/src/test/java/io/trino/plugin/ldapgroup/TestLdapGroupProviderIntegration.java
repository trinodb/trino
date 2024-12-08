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
import com.google.common.collect.ObjectArrays;
import com.google.common.io.Closer;
import io.trino.spi.security.GroupProvider;
import io.trino.testing.containers.TestingOpenLdapServer;
import io.trino.testing.containers.TestingOpenLdapServer.DisposableSubContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.Network;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestLdapGroupProviderIntegration
{
    private final LdapGroupProviderFactory factory = new LdapGroupProviderFactory();

    private Closer closer;

    private Map<String, String> baseConfig;

    ConfigBuilder cacheDisabledWithMemberOf = builder -> builder
            .put("cache.enabled", "false")
            .put("ldap.user-member-of-attribute", "memberOf");

    ConfigBuilder cacheDisabledWithGroupFilter = builder -> builder
            .put("cache.enabled", "false")
            .put("ldap.use-group-filter", "true")
            .put("ldap.group-base-dn", "ou=groups,dc=trino,dc=testldap,dc=com");

    ConfigBuilder cacheEnabledWithMemberOf = builder -> builder
            .put("cache.enabled", "true")
            .put("cache.ttl", "5s")
            .put("cache.maximum-size", "10")
            .put("ldap.user-member-of-attribute", "memberOf");

    ConfigBuilder cacheEnabledWithGroupFilter = builder -> builder
            .put("cache.enabled", "true")
            .put("cache.ttl", "5s")
            .put("cache.maximum-size", "10")
            .put("ldap.use-group-filter", "true")
            .put("ldap.group-base-dn", "ou=groups,dc=trino,dc=testldap,dc=com");

    private DisposableSubContext clients;
    private DisposableSubContext developers;
    private DisposableSubContext qa;

    @BeforeAll
    public void setup() throws Exception
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

        qa = openLdapServer.createGroup(groupsOU, "qa");
        openLdapServer.addUserToGroup(alicea, qa);
        openLdapServer.addUserToGroup(bobq, qa);
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
        assertGetGroups(cacheDisabledWithMemberOf, "alicea", ImmutableSet.of("clients", "developers", "qa"));
        assertGetGroups(cacheDisabledWithMemberOf, "johnb", ImmutableSet.of("clients"));
        assertGetGroups(cacheDisabledWithMemberOf, "bobq", ImmutableSet.of("qa"));
        assertGetGroups(cacheDisabledWithMemberOf, "carlp", ImmutableSet.of());
        assertGetGroups(cacheDisabledWithGroupFilter, "alicea", ImmutableSet.of("clients", "developers", "qa"));
        assertGetGroups(cacheDisabledWithGroupFilter, "johnb", ImmutableSet.of("clients"));
        assertGetGroups(cacheDisabledWithGroupFilter, "bobq", ImmutableSet.of("qa"));
        assertGetGroups(cacheDisabledWithGroupFilter, "carlp", ImmutableSet.of());
        assertGetGroups(cacheEnabledWithMemberOf, "alicea", ImmutableSet.of("clients", "developers", "qa"));
        assertGetGroups(cacheEnabledWithMemberOf, "johnb", ImmutableSet.of("clients"));
        assertGetGroups(cacheEnabledWithMemberOf, "bobq", ImmutableSet.of("qa"));
        assertGetGroups(cacheEnabledWithMemberOf, "carlp", ImmutableSet.of());
        assertGetGroups(cacheEnabledWithGroupFilter, "alicea", ImmutableSet.of("clients", "developers", "qa"));
        assertGetGroups(cacheEnabledWithGroupFilter, "johnb", ImmutableSet.of("clients"));
        assertGetGroups(cacheEnabledWithGroupFilter, "bobq", ImmutableSet.of("qa"));
        assertGetGroups(cacheEnabledWithGroupFilter, "carlp", ImmutableSet.of());
    }

    private void assertGetGroups(ConfigBuilder configBuilder, String userName, Set<String> expectedGroups)
    {
        Map<String, String> config = configBuilder.apply(ImmutableMap.<String, String>builder().putAll(baseConfig)).buildOrThrow();
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups(userName);

        assertThat(groups).containsAll(expectedGroups);
    }

    @Test
    public void testGetGroupsWithGroupsFilter()
    {
        assertGetGroupsWithGroupsFilter("alicea", "cn=*", ImmutableSet.of("clients", "developers", "qa"));
        assertGetGroupsWithGroupsFilter("alicea", "cn=dev*", ImmutableSet.of("developers"));
        assertGetGroupsWithGroupsFilter("alicea", "(|(cn=dev*)(cn=cl*))", ImmutableSet.of("developers", "clients"));
        assertGetGroupsWithGroupsFilter("alicea", "(&(objectclass=groupOfNames)(!(ou:dn:=external)))", ImmutableSet.of("developers", "qa"));
    }

    private void assertGetGroupsWithGroupsFilter(String userName, String groupFiler, Set<String> expectedGroups)
    {
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .putAll(baseConfig)
                .put("ldap.use-group-filter", "true")
                .put("ldap.group-search-member-attribute", "member")
                .put("ldap.group-name-attribute", "cn")
                .put("ldap.group-base-dn", "ou=groups,dc=trino,dc=testldap,dc=com")
                .put("ldap.group-search-filter", groupFiler)
                .buildOrThrow();
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups(userName);

        assertThat(groups).containsAll(expectedGroups);
    }

    @Test
    public void testGetGroupForMissingUserReturnsEmpty()
    {
        assertGetGroupForMissingUserReturnsEmpty(cacheEnabledWithMemberOf);
        assertGetGroupForMissingUserReturnsEmpty(cacheDisabledWithGroupFilter);
        assertGetGroupForMissingUserReturnsEmpty(cacheEnabledWithMemberOf);
        assertGetGroupForMissingUserReturnsEmpty(cacheEnabledWithGroupFilter);
    }

    private void assertGetGroupForMissingUserReturnsEmpty(ConfigBuilder configBuilder)
    {
        Map<String, String> config = configBuilder.apply(
                ImmutableMap.<String, String>builder()
                    .putAll(baseConfig))
                .buildOrThrow();
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
    public void testGetGroupsWithBadGroupNameReturnsFullName() {
        assertGetGroupsWithBadGroupNameReturnsFullName(cacheDisabledWithMemberOf);
        assertGetGroupsWithBadGroupNameReturnsFullName(cacheDisabledWithGroupFilter);
        assertGetGroupsWithBadGroupNameReturnsFullName(cacheEnabledWithMemberOf);
        assertGetGroupsWithBadGroupNameReturnsFullName(cacheEnabledWithGroupFilter);
    }

    private void assertGetGroupsWithBadGroupNameReturnsFullName(ConfigBuilder configBuilder)
    {
        Map<String, String> config = configBuilder.apply(
                ImmutableMap.<String, String>builder()
                        .put("ldap.group-name-attribute", "some-attribute-that-does-not-exist")
                        .putAll(baseConfig))
                .buildOrThrow();
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups("alicea");

        assertThat(groups).containsAll(ImmutableSet.of(clients.getDistinguishedName(), developers.getDistinguishedName(), qa.getDistinguishedName()));
    }

    @Test
    public void testGetGroupsConcurrently() throws InterruptedException {
        assertGetGroupsConcurrently(cacheDisabledWithMemberOf);
        assertGetGroupsConcurrently(cacheDisabledWithGroupFilter);
        assertGetGroupsConcurrently(cacheEnabledWithMemberOf);
        assertGetGroupsConcurrently(cacheEnabledWithGroupFilter);
    }

    private void assertGetGroupsConcurrently(ConfigBuilder configBuilder)
            throws InterruptedException
    {
        Map<String, String> config = configBuilder.apply(
                ImmutableMap.<String, String>builder()
                    .putAll(baseConfig))
                .buildOrThrow();
        GroupProvider groupsProvider = factory.create(config);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        CountDownLatch latch = new CountDownLatch(4);

        CompletableFuture.supplyAsync(() -> groupsProvider.getGroups("alicea"), executor).whenComplete((g, t) -> {
            assertThat(g).containsAll(ImmutableSet.of("clients", "qa", "developers"));
            latch.countDown();
        });

        CompletableFuture.supplyAsync(() -> groupsProvider.getGroups("bobq"), executor).whenComplete((g, t) -> {
            assertThat(g).containsAll(ImmutableSet.of("qa"));
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
        ImmutableMap.Builder<String, String> apply(ImmutableMap.Builder<String, String> builder);
    }
}
