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

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchResult;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class LdapFilteringGroupProvider
        implements GroupProvider
{
    private static final Logger log = Logger.get(LdapFilteringGroupProvider.class);

    private static final String LDAP_MATCHING_RULE_IN_CHAIN = "1.2.840.113556.1.4.1941";

    private final LdapClient ldapClient;
    private final String ldapAdminUser;
    private final String ldapAdminPassword;
    private final String userBaseDN;
    private final String userSearchFilter;
    private final String groupBaseDN;
    private final String groupsNameAttribute;
    private final String combinedGroupSearchFilter;
    private final boolean enableNestedGroups;
    private final boolean useMatchingRuleInChain;

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
        this.enableNestedGroups = filteringConfig.isLdapGroupSearchEnableNestedGroups();
        this.useMatchingRuleInChain = filteringConfig.isLdapGroupSearchUseMatchingRuleInChain();
        String groupSearchMemberPredicate = useMatchingRuleInChain
                ? String.format("%s:%s:={0}", groupsSearchMemberAttribute, LDAP_MATCHING_RULE_IN_CHAIN)
                : String.format("%s={0}", groupsSearchMemberAttribute);

        combinedGroupSearchFilter = filteringConfig.getLdapGroupsSearchFilter()
                .map(filter -> String.format("(&(%s)(%s))", filter, groupSearchMemberPredicate))
                .orElse(String.format("(%s)", groupSearchMemberPredicate));
    }

    /**
     * Resolve direct and nested LDAP groups for the provided user.
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

        return userDistinguishedName
                .map(ldapUser -> {
                    if (!enableNestedGroups) {
                        return resolveDirectGroups(ldapUser);
                    }
                    return useMatchingRuleInChain ? resolveGroupsInSingleQuery(ldapUser) : resolveGroupsRecursively(ldapUser);
                })
                .orElse(ImmutableSet.of());
    }

    private Set<String> resolveDirectGroups(String memberDistinguishedName)
    {
        try {
            return ldapClient.executeLdapQuery(ldapAdminUser, ldapAdminPassword,
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
                        return extractGroups(search).stream()
                                .map(LdapGroup::name)
                                .collect(toImmutableSet());
                    });
        }
        catch (NamingException e) {
            log.error(e, "LDAP group search for member [%s] failed", memberDistinguishedName);
            return ImmutableSet.of();
        }
    }

    private Set<String> resolveGroupsRecursively(String memberDistinguishedName)
    {
        Queue<String> membersToResolve = new ArrayDeque<>();
        membersToResolve.add(memberDistinguishedName);
        Set<String> visitedMembers = new HashSet<>();
        Set<String> groups = new HashSet<>();

        while (!membersToResolve.isEmpty()) {
            String currentMember = membersToResolve.remove();
            if (!visitedMembers.add(currentMember)) {
                continue;
            }

            try {
                Set<LdapGroup> groupsForMember = ldapClient.executeLdapQuery(ldapAdminUser, ldapAdminPassword,
                        new LdapQuery.LdapQueryBuilder()
                                .withSearchBase(groupBaseDN)
                                .withAttributes(groupsNameAttribute)
                                .withSearchFilter(combinedGroupSearchFilter)
                                .withFilterArguments(currentMember)
                                .build(),
                                search -> {
                                    if (!search.hasMore()) {
                                        log.debug("No groups found using search [pattern=%s, arguments={%s}]", combinedGroupSearchFilter, currentMember);
                                    }
                                    return extractGroups(search);
                                });

                groupsForMember.forEach(group -> {
                    groups.add(group.name());
                    membersToResolve.add(group.distinguishedName());
                });
            }
            catch (NamingException e) {
                log.error(e, "LDAP group search for member [%s] failed", currentMember);
                return ImmutableSet.of();
            }
        }

        return ImmutableSet.copyOf(groups);
    }

    private Set<String> resolveGroupsInSingleQuery(String memberDistinguishedName)
    {
        try {
            return ldapClient.executeLdapQuery(ldapAdminUser, ldapAdminPassword,
                    new LdapQuery.LdapQueryBuilder()
                            .withSearchBase(groupBaseDN)
                            .withAttributes(groupsNameAttribute)
                            .withSearchFilter(combinedGroupSearchFilter)
                            .withFilterArguments(memberDistinguishedName)
                            .build(),
                    search -> {
                        if (!search.hasMore()) {
                            log.debug("No groups found using LDAP_MATCHING_RULE_IN_CHAIN search [pattern=%s, arguments={%s}]", combinedGroupSearchFilter, memberDistinguishedName);
                        }
                        return extractGroups(search).stream()
                                .map(LdapGroup::name)
                                .collect(toImmutableSet());
                    });
        }
        catch (NamingException e) {
            log.error(e, "LDAP group search for member [%s] failed", memberDistinguishedName);
            return ImmutableSet.of();
        }
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

    private record LdapGroup(String distinguishedName, String name) {}
}
