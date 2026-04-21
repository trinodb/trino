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
import io.trino.spi.security.GroupProvider;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchResult;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class LdapFilteringGroupProvider
        implements GroupProvider
{
    private static final Logger log = Logger.get(LdapFilteringGroupProvider.class);

    private final LdapClient ldapClient;
    private final String ldapAdminUser;
    private final String ldapAdminPassword;
    private final String userBaseDN;
    private final String userSearchFilter;
    private final String groupBaseDN;
    private final String groupsNameAttribute;
    private final String combinedGroupSearchFilter;

    @Inject
    public LdapFilteringGroupProvider(
            LdapClient ldapClient,
            LdapGroupProviderConfig config,
            LdapFilteringGroupProviderConfig filteringConfig)
    {
        this.ldapClient = requireNonNull(ldapClient, "ldapClient is null");
        this.ldapAdminUser = config.getLdapAdminUser();
        this.ldapAdminPassword = config.getLdapAdminPassword();
        this.userBaseDN = config.getLdapUserBaseDN();
        this.userSearchFilter = config.getLdapUserSearchFilter();
        this.groupBaseDN = filteringConfig.getLdapGroupBaseDN();
        this.groupsNameAttribute = config.getLdapGroupsNameAttribute();

        String groupsSearchMemberAttribute = filteringConfig.getLdapGroupsSearchMemberAttribute();
        combinedGroupSearchFilter = filteringConfig.getLdapGroupsSearchFilter()
                .map(filter -> String.format("(&(%s)(%s={0}))", filter, groupsSearchMemberAttribute))
                .orElse(String.format("(%s={0})", groupsSearchMemberAttribute));
    }

    /**
     * Perform an LDAP search for groups, fetching only the names, and returning the name of each group.
     * Filters groups by user membership AND filter expression {@link LdapFilteringGroupProviderConfig#getLdapGroupsSearchFilter()}.
     * If {@link LdapGroupProviderConfig#getLdapGroupsNameAttribute()} is missing from group document, fallback on full name.
     * Swallows LDAP exceptions.
     *
     * @return Names of groups that the user is a member of
     */
    @Override
    public Set<String> getGroups(String user)
    {
        Optional<String> userDistinguishedName;
        try {
            userDistinguishedName = ldapClient.executeLdapQuery(ldapAdminUser, ldapAdminPassword,
                    new LdapQuery.LdapQueryBuilder()
                            .withSearchBase(userBaseDN)
                            .withSearchFilter(userSearchFilter)
                            .withFilterArguments(user)
                            .build(),
                    search -> {
                        if (!search.hasMore()) {
                            log.warn("LDAP search for user [%s] using filter pattern [%s] found no matches", user, userSearchFilter);
                            return Optional.empty();
                        }
                        SearchResult result = search.next();
                        return Optional.of(result.getNameInNamespace());
                    });
        }
        catch (NamingException e) {
            log.error(e, "LDAP search for user [%s] failed", user);
            return ImmutableSet.of();
        }

        return userDistinguishedName.map(ldapUser -> {
            try {
                return ldapClient.executeLdapQuery(ldapAdminUser, ldapAdminPassword,
                        new LdapQuery.LdapQueryBuilder()
                                .withSearchBase(groupBaseDN)
                                .withAttributes(groupsNameAttribute)
                                .withSearchFilter(combinedGroupSearchFilter)
                                .withFilterArguments(ldapUser)
                                .build(),
                        search -> {
                            if (!search.hasMore()) {
                                log.debug("No groups found using search [pattern=%s, arguments={%s}]", combinedGroupSearchFilter, ldapUser);
                            }
                            ImmutableSet.Builder<String> groupsBuilder = ImmutableSet.builder();
                            while (search.hasMore()) {
                                SearchResult groupResult = search.next();
                                Attribute groupName = groupResult.getAttributes().get(groupsNameAttribute);
                                if (groupName == null) {
                                    log.warn("The group object [%s] does not have group name attribute [%s]. Falling back on object full name.", groupResult, groupsNameAttribute);
                                    groupsBuilder.add(groupResult.getNameInNamespace());
                                }
                                else {
                                    groupsBuilder.add(groupName.get().toString());
                                }
                            }
                            return groupsBuilder.build();
                        });
            }
            catch (NamingException e) {
                log.error(e, "LDAP search for user [%s] groups failed", user);
                return ImmutableSet.<String>of();
            }
        }).orElse(ImmutableSet.of());
    }
}
