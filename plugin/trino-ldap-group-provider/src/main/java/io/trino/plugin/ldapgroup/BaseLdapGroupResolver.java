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
import io.airlift.log.Logger;
import io.trino.plugin.base.ldap.LdapClient;
import io.trino.plugin.base.ldap.LdapQuery;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchResult;

import java.util.Set;

import static java.util.Objects.requireNonNull;

abstract class BaseLdapGroupResolver
        implements LdapGroupResolver
{
    protected static final Logger log = Logger.get(LdapFilteringGroupProvider.class);

    protected final LdapClient ldapClient;
    protected final String ldapAdminUser;
    protected final String ldapAdminPassword;
    protected final String groupBaseDN;
    protected final String groupsNameAttribute;
    protected final String combinedGroupSearchFilter;

    protected BaseLdapGroupResolver(
            LdapClient ldapClient,
            LdapGroupProviderConfig config,
            LdapFilteringGroupProviderConfig filteringConfig,
            String groupSearchMemberPredicate)
    {
        this.ldapClient = requireNonNull(ldapClient, "ldapClient is null");
        this.ldapAdminUser = config.getLdapAdminUser();
        this.ldapAdminPassword = config.getLdapAdminPassword();
        this.groupBaseDN = filteringConfig.getLdapGroupBaseDN();
        this.groupsNameAttribute = config.getLdapGroupsNameAttribute();
        this.combinedGroupSearchFilter = filteringConfig.getLdapGroupsSearchFilter()
                .map(filter -> String.format("(&(%s)(%s))", filter, groupSearchMemberPredicate))
                .orElse(String.format("(%s)", groupSearchMemberPredicate));
    }

    protected Set<LdapGroup> searchGroups(String memberDistinguishedName)
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
                        log.debug("No groups found using %s [pattern=%s, arguments={%s}]", searchDescription(), combinedGroupSearchFilter, memberDistinguishedName);
                    }
                    return extractGroups(search);
                });
    }

    protected String searchDescription()
    {
        return "search";
    }

    private Set<LdapGroup> extractGroups(NamingEnumeration<SearchResult> search)
            throws NamingException
    {
        ImmutableSet.Builder<LdapGroup> groups = ImmutableSet.builder();
        boolean missingConfiguredNameAttribute = false;
        while (search.hasMore()) {
            SearchResult groupResult = search.next();
            Attribute groupName = groupResult.getAttributes().get(groupsNameAttribute);
            if (groupName == null) {
                missingConfiguredNameAttribute = true;
                log.debug("The group object [%s] does not have group name attribute [%s]. Falling back on object full name.", groupResult, groupsNameAttribute);
                groups.add(new LdapGroup(groupResult.getNameInNamespace(), groupResult.getNameInNamespace()));
            }
            else {
                groups.add(new LdapGroup(groupResult.getNameInNamespace(), groupName.get().toString()));
            }
        }
        if (missingConfiguredNameAttribute) {
            log.warn("Some LDAP group objects do not have configured group name attribute [%s]. Falling back on object full name.", groupsNameAttribute);
        }
        return groups.build();
    }

    protected record LdapGroup(String distinguishedName, String name) {}
}
