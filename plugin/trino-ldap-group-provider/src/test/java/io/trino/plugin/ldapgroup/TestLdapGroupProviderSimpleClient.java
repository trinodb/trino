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

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.ldapgroup.TestingLdapClient.assertLdapQueryEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;

@TestInstance(PER_METHOD)
public class TestLdapGroupProviderSimpleClient
{
    private static final String BASE_DN = "ou=testing,o=trino.io";
    private static final String USER = "test-trino-user";
    private static final String FULL_NAME = String.format("cn=%s,ou=users,%s", USER, BASE_DN);

    private LdapGroupProviderConfig config;
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
        mockClient = new TestingLdapClient();
    }

    @Test
    public void testGetUser()
            throws NamingException
    {
        BasicAttributes attributes = new BasicAttributes("uuid", 1337);
        attributes.put(createMultivaluesAttribute("memberOf", "cn=admin,ou=testing,o=trino.io", "cn=supervisor,ou=testing,o=trino.io"));
        mockClient.setSearchResult(createUserSearchResponse(attributes));

        config.setLdapUserSearchAttributes("uuid,memberOf");
        LdapGroupProviderClient ldapClient = new LdapGroupProviderSimpleClient(mockClient, config);
        Optional<LdapSearchUserResult> result = ldapClient.getUser(USER);

        assertTrue(result.isPresent(), "User not found");
        LdapSearchUserResult user = result.get();
        assertEquals(user.userDN(), FULL_NAME);
        assertEquals(user.userAttributes(), Map.of(
                "uuid", List.of("1337"),
                "memberOf", List.of("cn=admin,ou=testing,o=trino.io", "cn=supervisor,ou=testing,o=trino.io")));

        assertLdapQueryEquals(
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(BASE_DN)
                        .withSearchFilter("userName={0}")
                        .withFilterArguments(USER)
                        .withAttributes("uuid", "memberOf").build(),
                mockClient.getLdapQuery());
    }

    @Test
    public void testGetUserReturnsOnlySpecifiedAttributes()
            throws NamingException
    {
        BasicAttributes attributes = new BasicAttributes("uuid", 1337);
        attributes.put(createMultivaluesAttribute("logins", "2023-10-30", "2023-10-29"));
        attributes.put("terminal", "bash");
        mockClient.setSearchResult(createUserSearchResponse(attributes));

        LdapGroupProviderClient ldapClient = new LdapGroupProviderSimpleClient(mockClient, config);
        Optional<LdapSearchUserResult> result = ldapClient.getUser(USER);

        assertTrue(result.isPresent(), "User not found");
        LdapSearchUserResult user = result.get();
        assertEquals(user.userDN(), FULL_NAME);
        assertEquals(user.userAttributes(), Map.of("uuid", List.of("1337")));

        assertLdapQueryEquals(
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(BASE_DN)
                        .withSearchFilter("userName={0}")
                        .withFilterArguments(USER)
                        .withAttributes("uuid").build(),
                mockClient.getLdapQuery());
    }

    @Test
    public void testGetUserIgnoresMissingAttributes()
            throws NamingException
    {
        mockClient.setSearchResult(createUserSearchResponse(new BasicAttributes("uuid", 1337)));

        config.setLdapUserSearchAttributes("uuid,memberOf");
        LdapGroupProviderClient ldapClient = new LdapGroupProviderSimpleClient(mockClient, config);
        Optional<LdapSearchUserResult> result = ldapClient.getUser(USER);

        assertTrue(result.isPresent(), "User not found");
        LdapSearchUserResult user = result.get();
        assertEquals(user.userDN(), FULL_NAME);
        assertEquals(user.userAttributes(), Map.of("uuid", List.of("1337")));

        assertLdapQueryEquals(
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(BASE_DN)
                        .withSearchFilter("userName={0}")
                        .withFilterArguments(USER)
                        .withAttributes("uuid", "memberOf").build(),
                mockClient.getLdapQuery());
    }

    @Test
    public void testGetUserIgnoresAttributesWithNoValues()
            throws NamingException
    {
        BasicAttributes attributes = new BasicAttributes();
        attributes.put(new BasicAttribute("memberOf"));
        mockClient.setSearchResult(createUserSearchResponse(attributes));

        config.setLdapUserSearchAttributes("memberOf");
        LdapGroupProviderClient ldapClient = new LdapGroupProviderSimpleClient(mockClient, config);
        Optional<LdapSearchUserResult> result = ldapClient.getUser(USER);

        assertTrue(result.isPresent(), "User not found");
        LdapSearchUserResult user = result.get();
        assertEquals(user.userDN(), FULL_NAME);
        assertEquals(user.userAttributes(), Map.of());

        assertLdapQueryEquals(
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(BASE_DN)
                        .withSearchFilter("userName={0}")
                        .withFilterArguments(USER)
                        .withAttributes("memberOf").build(),
                mockClient.getLdapQuery());
    }

    @Test
    public void testGetUserReturnsEmptyWhenNotFound()
            throws NamingException
    {
        mockClient.setSearchResult(TestingNamingEnumeration.<SearchResult>of());

        config.setLdapUserSearchAttributes("uuid,memberOf");
        LdapGroupProviderClient ldapClient = new LdapGroupProviderSimpleClient(mockClient, config);
        Optional<LdapSearchUserResult> result = ldapClient.getUser(USER);

        assertTrue(result.isEmpty());

        assertLdapQueryEquals(
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(BASE_DN)
                        .withSearchFilter("userName={0}")
                        .withFilterArguments(USER)
                        .withAttributes("uuid", "memberOf").build(),
                mockClient.getLdapQuery());
    }

    @Test
    public void testGetUserReturnsFirstUserWhenMultipleMatched()
            throws NamingException
    {
        TestingNamingEnumeration<SearchResult> searchResponse = TestingNamingEnumeration.of(
                createUserSearchResult(USER, new BasicAttributes("uuid", 1337)),
                createUserSearchResult("wrong-user", new BasicAttributes("uuid", 42)));
        mockClient.setSearchResult(searchResponse);

        LdapGroupProviderClient ldapClient = new LdapGroupProviderSimpleClient(mockClient, config);
        Optional<LdapSearchUserResult> result = ldapClient.getUser(USER);

        assertTrue(result.isPresent(), "User not found");
        LdapSearchUserResult user = result.get();
        assertEquals(user.userDN(), FULL_NAME);
        assertEquals(user.userAttributes(), Map.of("uuid", List.of("1337")));

        assertLdapQueryEquals(
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(BASE_DN)
                        .withSearchFilter("userName={0}")
                        .withFilterArguments(USER)
                        .withAttributes("uuid").build(),
                mockClient.getLdapQuery());
    }

    @Test
    public void testGetGroupsParsesGroupNames()
            throws NamingException
    {
        LdapSearchUserResult searchUserResult = new LdapSearchUserResult(USER,
                Map.of("memberOf", List.of("cn=admin,ou=testing,o=trino.io", "cn=supervisor,ou=testing,o=trino.io")));

        config.setLdapUserMemberOfAttribute("memberOf");
        LdapGroupProviderClient ldapClient = new LdapGroupProviderSimpleClient(mockClient, config);
        List<String> result = ldapClient.getGroups(searchUserResult);

        assertEquals(List.of("admin", "supervisor"), result);
        assertNull(mockClient.getLdapQuery());
    }

    @Test
    public void testGetGroupsIgnoresMalformedGroupNames()
            throws NamingException
    {
        LdapSearchUserResult searchUserResult = new LdapSearchUserResult(USER,
                Map.of("memberOf", List.of("cn=admin,ou=testing,o=trino.io", "=user,ou=testing,o=trino.io", "client")));

        config.setLdapUserMemberOfAttribute("memberOf");
        LdapGroupProviderClient ldapClient = new LdapGroupProviderSimpleClient(mockClient, config);
        List<String> result = ldapClient.getGroups(searchUserResult);

        assertEquals(List.of("admin"), result);
        assertNull(mockClient.getLdapQuery());
    }

    @Test
    public void testGetGroupsUsesDNWhenLastRDNDoesNotMatch()
            throws NamingException
    {
        LdapSearchUserResult searchUserResult = new LdapSearchUserResult(USER,
                Map.of("memberOf", List.of("cn=admin,ou=testing,o=trino.io", "name=user,ou=testing,o=trino.io")));

        config.setLdapUserMemberOfAttribute("memberOf");
        LdapGroupProviderClient ldapClient = new LdapGroupProviderSimpleClient(mockClient, config);
        List<String> result = ldapClient.getGroups(searchUserResult);

        assertEquals(List.of("admin", "name=user,ou=testing,o=trino.io"), result);
        assertNull(mockClient.getLdapQuery());
    }

    @Test
    public void testGetUserRethrowsSearchException()
    {
        mockClient.setExceptionToThrow(new NamingException("Test naming exception"));

        LdapGroupProviderClient ldapClient = new LdapGroupProviderSimpleClient(mockClient, config);
        NamingException thrown = assertThrows(NamingException.class, () -> ldapClient.getUser(USER));

        assertEquals(thrown.getMessage(), "Test naming exception");
    }

    private static NamingEnumeration<SearchResult> createUserSearchResponse(Attributes attributes)
    {
        SearchResult result = new SearchResult(FULL_NAME, FULL_NAME, attributes);
        result.setNameInNamespace(FULL_NAME);
        return TestingNamingEnumeration.of(result);
    }

    private static SearchResult createUserSearchResult(String name, Attributes attributes)
    {
        String fullName = String.format("cn=%s,ou=users,%s", name, BASE_DN);
        SearchResult result = new SearchResult(fullName, fullName, attributes);
        result.setNameInNamespace(fullName);
        return result;
    }

    private static Attribute createMultivaluesAttribute(String name, Object... values)
    {
        BasicAttribute attribute = new BasicAttribute(name);
        for (Object value : values) {
            attribute.add(value);
        }
        return attribute;
    }
}
