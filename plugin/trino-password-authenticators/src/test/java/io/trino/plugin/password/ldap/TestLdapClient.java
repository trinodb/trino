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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.password.Credential;

import javax.naming.NamingException;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class TestLdapClient
        implements LdapClient
{
    static final String BASE_DN = "base-dn";
    static final String USER_BASE_DN = "user-ou," + BASE_DN;
    static final String GROUP_BASE_DN = "group-ou," + BASE_DN;
    static final String PATTERN_PREFIX = "pattern::";
    static final String DEFAULT_GROUP_NAME = "default";

    private final Set<Credential> credentials = new HashSet<>();
    private final Set<String> groupMembers = new HashSet<>();
    private final HashMultimap<String, String> userDNs = HashMultimap.create();

    public void addCredentials(String userDistinguishedName, String password)
    {
        credentials.add(new Credential(userDistinguishedName, password));
    }

    public void addGroupMember(String userName)
    {
        groupMembers.add(userName);
    }

    public void addDistinguishedNameForUser(String userName, String distinguishedName)
    {
        userDNs.put(userName, distinguishedName);
    }

    @Override
    public void validatePassword(String userDistinguishedName, String password)
            throws NamingException
    {
        if (!credentials.contains(new Credential(userDistinguishedName, password))) {
            throw new NamingException();
        }
    }

    @Override
    public boolean isGroupMember(String searchBase, String groupSearch, String contextUserDistinguishedName, String contextPassword)
            throws NamingException
    {
        validatePassword(contextUserDistinguishedName, contextPassword);
        return getSearchUser(searchBase, groupSearch)
                .map(groupMembers::contains)
                .orElse(false);
    }

    @Override
    public Set<String> lookupUserDistinguishedNames(String searchBase, String searchFilter, String contextUserDistinguishedName, String contextPassword)
            throws NamingException
    {
        validatePassword(contextUserDistinguishedName, contextPassword);

        if (searchFilter.startsWith(LdapGroupProvider.GROUP_SEARCH_FILTER_PREFIX)) {
            return getSearchGroup(searchBase, searchFilter)
                    .map(ImmutableSet::of)
                    .orElse(ImmutableSet.of());
        }
        else {
            return getSearchUser(searchBase, searchFilter)
                    .map(userDNs::get)
                    .orElse(ImmutableSet.of());
        }
    }

    private static Optional<String> getSearchUser(String searchBase, String groupSearch)
    {
        if (!searchBase.endsWith(BASE_DN)) {
            return Optional.empty();
        }

        if (!groupSearch.startsWith(PATTERN_PREFIX)) {
            return Optional.empty();
        }
        return Optional.of(groupSearch.substring(PATTERN_PREFIX.length()));
    }

    private Optional<String> getSearchGroup(String searchBase, String groupSearch)
    {
        if (!searchBase.endsWith(BASE_DN)) {
            return Optional.empty();
        }

        String userDn = groupSearch.substring(LdapGroupProvider.GROUP_SEARCH_FILTER_PREFIX.length());

        if (groupMembers.contains(userDn)) {
            return Optional.of("cn=" + DEFAULT_GROUP_NAME + "," + BASE_DN);
        }
        else {
            return Optional.empty();
        }
    }
}
