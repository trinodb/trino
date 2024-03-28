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

import io.trino.plugin.base.ldap.LdapClient;
import io.trino.plugin.base.ldap.LdapQuery;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchResult;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestingLdapClient
        implements LdapClient
{
    private NamingEnumeration<SearchResult> searchResult;
    private NamingException exceptionToThrow;
    private LdapQuery ldapQuery;

    public void setSearchResult(NamingEnumeration<SearchResult> searchResult)
    {
        this.searchResult = searchResult;
    }

    public void setExceptionToThrow(NamingException exceptionToThrow)
    {
        this.exceptionToThrow = exceptionToThrow;
    }

    public LdapQuery getLdapQuery()
    {
        return ldapQuery;
    }

    @Override
    public <T> T processLdapContext(String userName, String password, LdapContextProcessor<T> contextProcessor)
    {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public <T> T executeLdapQuery(String userName, String password, LdapQuery ldapQuery, LdapSearchResultProcessor<T> resultProcessor)
            throws NamingException
    {
        this.ldapQuery = ldapQuery;
        if (exceptionToThrow != null) {
            throw exceptionToThrow;
        }
        return resultProcessor.process(searchResult);
    }

    public static void assertLdapQueryEquals(LdapQuery expected, LdapQuery actual)
    {
        assertEquals(expected.getSearchBase(), actual.getSearchBase());
        assertEquals(expected.getSearchFilter(), actual.getSearchFilter());
        assertEquals(expected.getAttributes(), expected.getAttributes());
        assertEquals(expected.getFilterArguments(), expected.getFilterArguments());
    }
}
