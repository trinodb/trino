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

import io.trino.plugin.base.ldap.LdapQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.naming.NamingException;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestLdapGroupProviderFilteringClient
{
    private static final String BASE_DN = "ou=testing,o=trino.io";
    private static final String USER = "test-trino-user";
    private static final String FULL_NAME = String.format("cn=%s,ou=users,%s", USER, BASE_DN);
    private static final LdapSearchUserResult SEARCH_RESULT = new LdapSearchUserResult(FULL_NAME, Map.of());

    private LdapGroupProviderConfig config;
    private LdapGroupProviderFilteringClientConfig filteringConfig;
    private TestingLdapClient mockClient;

    @BeforeEach
    public void setup()
    {
        config = new LdapGroupProviderConfig()
                .setLdapAdminUser("admin")
                .setLdapAdminPassword("pass")
                .setLdapUserBaseDN("ou=testing,o=trino.io")
                .setLdapUserSearchFilter("userName={0}")
                .setLdapUserSearchAttributes("uuid")
                .setLdapGroupsNameAttribute("cn");
        filteringConfig = new LdapGroupProviderFilteringClientConfig()
                .setLdapGroupBaseDN("ou=testing,o=trino.io")
                .setLdapGroupsSearchMemberAttribute("member");
        mockClient = new TestingLdapClient();
    }

    @Test
    public void testGetUserCallsDelegate()
            throws NamingException
    {
        Optional<LdapSearchUserResult> sentinelSearchUserResult = Optional.of(new LdapSearchUserResult(USER, Map.of("attrib-1", List.of("val-1"))));
        LdapGroupProviderClient mockDelegateClient = new MockLdapGroupProviderClient()
                .returnSearchUserResult(sentinelSearchUserResult);

        LdapGroupProviderClient ldapClient = new LdapGroupProviderFilteringClient(mockClient, mockDelegateClient, config, filteringConfig);
        Optional<LdapSearchUserResult> result = ldapClient.getUser(USER);

        assertEquals(sentinelSearchUserResult, result);
    }

    @Test
    public void testGetGroups()
            throws NamingException
    {
        mockClient.setSearchResult(TestingNamingEnumeration.of(createGroupSearchResult("group_1"), createGroupSearchResult("group_2")));

        LdapGroupProviderClient ldapClient = new LdapGroupProviderFilteringClient(mockClient, new LdapGroupProviderSimpleClient(mockClient, config), config, filteringConfig);
        List<String> result = ldapClient.getGroups(SEARCH_RESULT);

        assertEquals(result, List.of("group_1", "group_2"));

        assertLdapQueryEquals(
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(BASE_DN)
                        .withSearchFilter("(member={0})")
                        .withFilterArguments(USER)
                        .withAttributes("cn").build(),
                mockClient.getLdapQuery());
    }

    @Test
    public void testGetGroupsWithFilter()
            throws NamingException
    {
        mockClient.setSearchResult(TestingNamingEnumeration.of(createGroupSearchResult("group_1")));

        filteringConfig.setLdapGroupsSearchFilter("cn=*_1");
        LdapGroupProviderClient ldapClient = new LdapGroupProviderFilteringClient(mockClient, new LdapGroupProviderSimpleClient(mockClient, config), config, filteringConfig);
        List<String> result = ldapClient.getGroups(SEARCH_RESULT);

        assertEquals(result, List.of("group_1"));

        assertLdapQueryEquals(
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(BASE_DN)
                        .withSearchFilter("(&(cn=*_1)(member={0}))")
                        .withFilterArguments(FULL_NAME)
                        .withAttributes("cn").build(),
                mockClient.getLdapQuery());
    }

    @Test
    public void testGetGroupsMissingNameFallsBackOnFullName()
            throws NamingException
    {
        mockClient.setSearchResult(TestingNamingEnumeration.of(
                createGroupSearchResult("group_1", "specialGroupName", "special_group_1"),
                createGroupSearchResult("group_2")));

        config.setLdapGroupsNameAttribute("specialGroupName");
        LdapGroupProviderClient ldapClient = new LdapGroupProviderFilteringClient(mockClient, new LdapGroupProviderSimpleClient(mockClient, config), config, filteringConfig);
        List<String> result = ldapClient.getGroups(SEARCH_RESULT);

        assertEquals(result, List.of("special_group_1", "cn=group_2,ou=groups," + BASE_DN));

        assertLdapQueryEquals(
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(BASE_DN)
                        .withSearchFilter("(member={0})")
                        .withFilterArguments(FULL_NAME)
                        .withAttributes("specialGroupName").build(),
                mockClient.getLdapQuery());
    }

    @Test
    public void testGetGroupsReturnsEmptyWhenNoMatches()
            throws NamingException
    {
        mockClient.setSearchResult(TestingNamingEnumeration.<SearchResult>of());

        LdapGroupProviderClient ldapClient = new LdapGroupProviderFilteringClient(mockClient, new LdapGroupProviderSimpleClient(mockClient, config), config, filteringConfig);
        List<String> result = ldapClient.getGroups(SEARCH_RESULT);

        assertEquals(result, List.of());

        assertLdapQueryEquals(
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(BASE_DN)
                        .withSearchFilter("(member={0})")
                        .withFilterArguments(FULL_NAME)
                        .withAttributes("cn").build(),
                mockClient.getLdapQuery());
    }

    @Test
    public void testGetGroupsRethrowsSearchException()
    {
        mockClient.setExceptionToThrow(new NamingException("Test naming exception"));

        LdapGroupProviderClient ldapClient = new LdapGroupProviderFilteringClient(mockClient, new LdapGroupProviderSimpleClient(mockClient, config), config, filteringConfig);
        NamingException ignored = assertThrows(NamingException.class, () -> ldapClient.getGroups(SEARCH_RESULT));

        assertLdapQueryEquals(
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(BASE_DN)
                        .withSearchFilter("(member={0})")
                        .withFilterArguments(FULL_NAME)
                        .withAttributes("cn").build(),
                mockClient.getLdapQuery());
    }

    private static SearchResult createGroupSearchResult(String name)
    {
        String groupFullName = String.format("cn=%s,ou=groups,%s", name, BASE_DN);
        SearchResult result = new SearchResult(groupFullName, groupFullName, new BasicAttributes("cn", name));
        result.setNameInNamespace(groupFullName);
        return result;
    }

    private static SearchResult createGroupSearchResult(String name, String attrId, Object value)
    {
        SearchResult result = createGroupSearchResult(name);
        result.getAttributes().put(attrId, value);
        return result;
    }

    private static void assertLdapQueryEquals(LdapQuery expected, LdapQuery actual)
    {
        assertEquals(expected.getSearchBase(), actual.getSearchBase());
        assertEquals(expected.getSearchFilter(), actual.getSearchFilter());
        assertEquals(expected.getAttributes(), expected.getAttributes());
        assertEquals(expected.getFilterArguments(), expected.getFilterArguments());
    }

    public static final class MockLdapGroupProviderClient
            implements LdapGroupProviderClient
    {
        private Optional<LdapSearchUserResult> searchUserResult;

        public MockLdapGroupProviderClient returnSearchUserResult(Optional<LdapSearchUserResult> searchUserResult)
        {
            this.searchUserResult = searchUserResult;
            return this;
        }

        @Override
        public Optional<LdapSearchUserResult> getUser(String user)
        {
            return searchUserResult;
        }

        @Override
        public List<String> getGroups(LdapSearchUserResult searchUserResult)
        {
            throw new RuntimeException("Not implemented");
        }
    }
}
