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
import com.google.common.collect.ObjectArrays;
import com.google.common.io.Closer;
import io.trino.spi.security.GroupProvider;
import io.trino.testing.containers.TestingOpenLdapServer;
import io.trino.testing.containers.TestingOpenLdapServer.DisposableSubContext;
import org.junit.jupiter.api.AfterAll;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestLdapGroupProviderIntegration
{
    private final LdapGroupProviderFactory factory = new LdapGroupProviderFactory();

    private final Closer closer;

    private final Map<String, String> baseConfig;

    private final DisposableSubContext clients;
    private final DisposableSubContext developers;
    private final DisposableSubContext qa;

    public TestLdapGroupProviderIntegration()
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

        DisposableSubContext johnb = openLdapServer.createUser(usersOU, "johnb", ImmutableMap.of(
                "employeeNumber", "1000",
                "mail", "johnb@company.com"));
        DisposableSubContext alicea = openLdapServer.createUser(usersOU, "alicea", ImmutableMap.of(
                "employeeNumber", "1001",
                "mail", "alicea@trino.io",
                "givenname", "Alice"));
        DisposableSubContext bobq = openLdapServer.createUser(usersOU, "bobq", ImmutableMap.of(
                "employeeNumber", "1002"));
        openLdapServer.createUser(usersOU, "carlp", ImmutableMap.of(
                "employeeNumber", "1003"));

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

    @ParameterizedTest
    @MethodSource("provideUsersAndExpectedGroups")
    public void testGetGroups(ConfigBuilder configBuilder, String userName, Set<String> expectedGroups)
    {
        Map<String, String> config = configBuilder.apply(ImmutableMap.<String, String>builder().putAll(baseConfig)).buildOrThrow();
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups(userName);

        assertEquals(expectedGroups, groups);
    }

    @ParameterizedTest
    @MethodSource("provideForTestGetGroupsAndUserAttributes")
    public void testGetGroupsWithUserAttributes(
            ConfigBuilder configBuilder,
            String userName,
            String searchAttribute,
            Set<String> expectedGroups)
    {
        Map<String, String> config = configBuilder.apply(
                ImmutableMap.<String, String>builder()
                    .putAll(baseConfig)
                    .put("ldap.user-search-attributes", searchAttribute))
                .buildOrThrow();
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups(userName);

        assertEquals(expectedGroups, groups);
    }

    @ParameterizedTest
    @MethodSource("provideForTestGetGroupsWithGroupsFilter")
    public void testGetGroupsWithGroupsFilter(String userName, String groupFiler, Set<String> expectedGroups)
    {
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .putAll(baseConfig)
                .put("ldap.group-search-member-attribute", "member")
                .put("ldap.group-name-attribute", "cn")
                .put("ldap.group-base-dn", "ou=groups,dc=trino,dc=testldap,dc=com")
                .put("ldap.group-search-filter", groupFiler)
                .buildOrThrow();
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups(userName);

        assertEquals(expectedGroups, groups);
    }

    @ParameterizedTest
    @MethodSource("provideConfigBuilders")
    public void testGetGroupForMissingUserReturnsEmpty(ConfigBuilder configBuilder)
    {
        Map<String, String> config = configBuilder.apply(
                ImmutableMap.<String, String>builder()
                    .putAll(baseConfig))
                .buildOrThrow();
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups("wrong-user-name");

        assertEquals(Set.of(), groups);
    }

    @Test
    public void testGetGroupsWithBadGroupMemberAttributeReturnsEmpty()
    {
        Map<String, String> config = ImmutableMap.<String, String>builder()
                .putAll(baseConfig)
                .put("ldap.group-search-member-attribute", "some-attribute-that-does-not-exist")
                .put("ldap.group-name-attribute", "cn")
                .put("ldap.group-base-dn", "ou=groups,dc=trino,dc=testldap,dc=com")
                .buildOrThrow();
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups("alicea");

        assertEquals(Set.of(), groups);
    }

    @ParameterizedTest
    @MethodSource("provideConfigBuilders")
    public void testGetGroupsWithBadGroupNameReturnsFullName(ConfigBuilder configBuilder)
    {
        Map<String, String> config = configBuilder.apply(
                ImmutableMap.<String, String>builder()
                        .put("ldap.group-name-attribute", "some-attribute-that-does-not-exist")
                        .putAll(baseConfig))
                .buildOrThrow();
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups("alicea");

        assertEquals(Set.of(clients.getDistinguishedName(), developers.getDistinguishedName(), qa.getDistinguishedName()), groups);
    }

    @ParameterizedTest
    @MethodSource("provideConfigBuilders")
    public void testGetGroupIgnoresMissingUserAttributes(ConfigBuilder configBuilder)
    {
        Map<String, String> config = configBuilder.apply(
                ImmutableMap.<String, String>builder()
                    .putAll(baseConfig)
                    .put("ldap.user-search-attributes", "employeeNumber,address"))
                .buildOrThrow();
        GroupProvider groupsProvider = factory.create(config);

        Set<String> groups = groupsProvider.getGroups("carlp");

        assertEquals(Set.of("employeeNumber=1003"), groups);
    }

    @ParameterizedTest
    @MethodSource("provideConfigBuilders")
    public void testGetGroupsConcurrently(ConfigBuilder configBuilder)
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
            assertEquals(g, Set.of("clients", "qa", "developers"));
            latch.countDown();
        });

        CompletableFuture.supplyAsync(() -> groupsProvider.getGroups("bobq"), executor).whenComplete((g, t) -> {
            assertEquals(g, Set.of("qa"));
            latch.countDown();
        });

        CompletableFuture.supplyAsync(() -> groupsProvider.getGroups("johnb"), executor).whenComplete((g, t) -> {
            assertEquals(g, Set.of("clients"));
            latch.countDown();
        });

        CompletableFuture.supplyAsync(() -> groupsProvider.getGroups("carlp"), executor).whenComplete((g, t) -> {
            assertEquals(g, Set.of());
            latch.countDown();
        });

        latch.await();
    }

    private static Stream<Arguments> combineArgumentStreams(Stream<Arguments> left, Stream<Arguments> right)
    {
        List<Arguments> rightArguments = right.collect(toImmutableList());
        return left.flatMap(l -> rightArguments.stream().map(r -> () -> ObjectArrays.concat(l.get(), r.get(), Object.class)));
    }

    private static Stream<Arguments> provideConfigBuilders()
    {
        return Stream.<ConfigBuilder>of(
                (builder) -> builder.put("ldap.user-member-of-attribute", "memberOf"),
                (builder) -> builder.put("ldap.group-base-dn", "ou=groups,dc=trino,dc=testldap,dc=com"))
                .map(Arguments::of);
    }

    private static Stream<Arguments> provideUsersAndExpectedGroups()
    {
        return combineArgumentStreams(provideConfigBuilders(), Stream.of(
                Arguments.of("alicea", Set.of("clients", "developers", "qa")),
                Arguments.of("johnb", Set.of("clients")),
                Arguments.of("bobq", Set.of("qa")),
                Arguments.of("carlp", Set.of())));
    }

    private static Stream<Arguments> provideForTestGetGroupsAndUserAttributes()
    {
        return combineArgumentStreams(provideConfigBuilders(), Stream.of(
                Arguments.of("alicea", "employeeNumber,mail", Set.of(
                        "clients",
                        "developers",
                        "qa",
                        "employeeNumber=1001",
                        "mail=alicea@trino.io")),
                Arguments.of("alicea", "givenname", Set.of("clients", "developers", "qa", "givenname=Alice")),
                Arguments.of("bobq", "employeeNumber,,mail", Set.of("qa", "employeeNumber=1002")),
                Arguments.of("johnb", "employeeNumber,mail", Set.of("clients", "employeeNumber=1000", "mail=johnb@company.com")),
                Arguments.of("carlp", "employeeNumber,mail", Set.of("employeeNumber=1003"))));
    }

    private static Stream<Arguments> provideForTestGetGroupsWithGroupsFilter()
    {
        return Stream.of(
                Arguments.of("alicea", "cn=*", Set.of("clients", "developers", "qa")),
                Arguments.of("alicea", "cn=dev*", Set.of("developers")),
                Arguments.of("alicea", "(|(cn=dev*)(cn=cl*))", Set.of("developers", "clients")),
                Arguments.of("alicea", "(&(objectclass=groupOfNames)(!(ou:dn:=external)))", Set.of("developers", "qa")));
    }

    @FunctionalInterface
    public interface ConfigBuilder
    {
        ImmutableMap.Builder<String, String> apply(ImmutableMap.Builder<String, String> builder);
    }
}
