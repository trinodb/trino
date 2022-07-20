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
package io.trino.plugin.base.ldap;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchResult;

public interface LdapClient
{
    <T> T processLdapContext(String userName, String password, LdapContextProcessor<T> contextProcessor)
            throws NamingException;

    <T> T executeLdapQuery(String userName, String password, LdapQuery ldapQuery, LdapSearchResultProcessor<T> resultProcessor)
            throws NamingException;

    interface LdapSearchResultProcessor<T>
    {
        T process(NamingEnumeration<SearchResult> searchResults)
                throws NamingException;
    }

    interface LdapContextProcessor<T>
    {
        T process(DirContext dirContext)
                throws NamingException;
    }
}
