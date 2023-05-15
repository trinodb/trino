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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.base.ldap.LdapClient;
import io.trino.plugin.base.ldap.LdapQuery;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class LdapAuthenticatorClient
{
    private final LdapClient ldapClient;

    @Inject
    public LdapAuthenticatorClient(LdapClient ldapClient)
    {
        this.ldapClient = requireNonNull(ldapClient, "ldapClient is null");
    }

    public void validatePassword(String userDistinguishedName, String password)
            throws NamingException
    {
        ldapClient.processLdapContext(userDistinguishedName, password, context -> null);
    }

    public boolean isGroupMember(String searchBase, String groupSearch, String contextUserDistinguishedName, String contextPassword)
            throws NamingException
    {
        return ldapClient.executeLdapQuery(
                contextUserDistinguishedName,
                contextPassword,
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(searchBase)
                        .withSearchFilter(groupSearch).build(),
                NamingEnumeration::hasMore);
    }

    public Set<String> lookupUserDistinguishedNames(String searchBase, String searchFilter, String contextUserDistinguishedName, String contextPassword)
            throws NamingException
    {
        return ldapClient.executeLdapQuery(
                contextUserDistinguishedName,
                contextPassword,
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(searchBase)
                        .withSearchFilter(searchFilter)
                        .build(),
                searchResults -> {
                    ImmutableSet.Builder<String> distinguishedNames = ImmutableSet.builder();
                    while (searchResults.hasMore()) {
                        distinguishedNames.add(searchResults.next().getNameInNamespace());
                    }
                    return distinguishedNames.build();
                });
    }
}
