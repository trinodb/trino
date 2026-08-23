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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.base.ldap.LdapClient;
import io.trino.plugin.base.ldap.LdapQuery;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchResult;

import java.util.Set;

import static java.util.Objects.requireNonNull;

final class LdapGroupSearch
{
    private static final Logger log = Logger.get(LdapFilteringGroupProvider.class);

    private final LdapClient ldapClient;
    private final String ldapAdminUser;
    private final String ldapAdminPassword;
    private final String groupBaseDN;
    private final String groupsNameAttribute;
    private final String combinedGroupSearchFilter;

    @Inject
    public LdapGroupSearch(
            LdapClient ldapClient,
            LdapGroupProviderConfig config,
            LdapFilteringGroupProviderConfig filteringConfig)
    {
        this.ldapClient = requireNonNull(ldapClient, "ldapClient is null");
        this.ldapAdminUser = config.getLdapAdminUser();
        this.ldapAdminPassword = config.getLdapAdminPassword();
        this.groupBaseDN = filteringConfig.getLdapGroupBaseDN();
        this.groupsNameAttribute = config.getLdapGroupsNameAttribute();

        String groupsSearchMemberAttribute = filteringConfig.getLdapGroupsSearchMemberAttribute();
        this.combinedGroupSearchFilter = filteringConfig.getLdapGroupsSearchFilter()
                .map(filter -> String.format("(&(%s)(%s={0}))", filter, groupsSearchMemberAttribute))
                .orElse(String.format("(%s={0})", groupsSearchMemberAttribute));
    }

    public Set<LdapGroup> searchGroups(String memberDistinguishedName)
            throws NamingException
    {
        return ldapClient.executeLdapQuery(
                ldapAdminUser,
                ldapAdminPassword,
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(groupBaseDN)
                        .withAttributes(groupsNameAttribute)
                        .withSearchFilter(combinedGroupSearchFilter)
                        .withFilterArguments(memberDistinguishedName)
                        .build(),
                search -> {
                    if (!search.hasMore()) {
                        log.debug("No groups found using search [pattern=%s, arguments={%s}]", combinedGroupSearchFilter, memberDistinguishedName);
                    }
                    return extractGroups(search);
                });
    }

    private Set<LdapGroup> extractGroups(NamingEnumeration<SearchResult> search)
            throws NamingException
    {
        ImmutableSet.Builder<LdapGroup> groups = ImmutableSet.builder();
        while (search.hasMore()) {
            SearchResult groupResult = search.next();
            Attribute groupName = groupResult.getAttributes().get(groupsNameAttribute);
            if (groupName == null) {
                log.warn("The group object [%s] does not have group name attribute [%s]. Falling back on object full name.", groupResult, groupsNameAttribute);
                groups.add(new LdapGroup(groupResult.getNameInNamespace(), groupResult.getNameInNamespace()));
            }
            else {
                groups.add(new LdapGroup(groupResult.getNameInNamespace(), groupName.get().toString()));
            }
        }
        return groups.build();
    }
}
