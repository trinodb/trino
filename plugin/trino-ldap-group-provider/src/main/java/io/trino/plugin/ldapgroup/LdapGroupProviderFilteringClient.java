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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.base.ldap.LdapClient;
import io.trino.plugin.base.ldap.LdapQuery;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchResult;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class LdapGroupProviderFilteringClient
        implements LdapGroupProviderClient
{
    private static final Logger log = Logger.get(LdapGroupProviderFilteringClient.class);

    private final LdapClient ldapClient;
    private final LdapGroupProviderClient userSearchDelegate;
    private final String ldapAdminUser;
    private final String ldapAdminPassword;
    private final String groupBaseDN;
    private final String groupsNameAttribute;
    private final String combinedGroupSearchFilter;

    @Inject
    public LdapGroupProviderFilteringClient(LdapClient ldapClient,
            @ForLdapUserSearchDelegate LdapGroupProviderClient userSearchDelegate,
            LdapGroupProviderConfig config,
            LdapGroupProviderFilteringClientConfig filteringConfig)
    {
        this.ldapClient = requireNonNull(ldapClient, "ldap client is null");
        this.userSearchDelegate = requireNonNull(userSearchDelegate, "ldap user search client is null");
        this.ldapAdminUser = config.getLdapAdminUser();
        this.ldapAdminPassword = config.getLdapAdminPassword();
        this.groupBaseDN = filteringConfig.getLdapGroupBaseDN();
        this.groupsNameAttribute = config.getLdapGroupsNameAttribute();

        String groupsSearchMemberAttribute = filteringConfig.getLdapGroupsSearchMemberAttribute();
        combinedGroupSearchFilter = filteringConfig.getLdapGroupsSearchFilter()
                .map(filter -> String.format("(&(%s)(%s={0}))", filter, groupsSearchMemberAttribute))
                .orElse(String.format("(%s={0})", groupsSearchMemberAttribute));
    }

    /**
     * See {@link LdapGroupProviderSimpleClient#getUser}
     */
    @Override
    public Optional<LdapSearchUserResult> getUser(String user)
            throws NamingException
    {
        return userSearchDelegate.getUser(user);
    }

    /**
     * Perform an LDAP search for groups, fetching only the names, and returning the name of each group.
     * Filters groups by user membership AND filter expression {@link LdapGroupProviderFilteringClientConfig#getLdapGroupsSearchFilter()}.
     * If {@link LdapGroupProviderConfig#getLdapGroupsNameAttribute()} is missing from group document, fallback on full name.
     *
     * @param searchUserResult User search result from {@link LdapGroupProviderClient#getUser}
     * {@link LdapGroupProviderFilteringClientConfig#getLdapGroupsSearchMemberAttribute()}
     * @return Names of groups that the user is a member of
     * @throws NamingException Underlying LDAP error
     */
    @Override
    public List<String> getGroups(LdapSearchUserResult searchUserResult)
            throws NamingException
    {
        return ldapClient.executeLdapQuery(ldapAdminUser, ldapAdminPassword,
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(groupBaseDN)
                        .withAttributes(groupsNameAttribute)
                        .withSearchFilter(combinedGroupSearchFilter)
                        .withFilterArguments(searchUserResult.userDN())
                        .build(),
                search -> {
                    if (!search.hasMore()) {
                        log.debug("No groups found using search [pattern=%s, arguments={%s}]", combinedGroupSearchFilter, searchUserResult.userDN());
                    }
                    ImmutableList.Builder<String> groupsBuilder = ImmutableList.builder();
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
}
