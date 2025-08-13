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

public class LdapChainGroupProvider
        implements GroupProvider
{
    private static final Logger log = Logger.get(LdapChainGroupProvider.class);

    private final LdapClient ldapClient;
    private final String ldapAdminUser;
    private final String ldapAdminPassword;
    private final String userBaseDN;
    private final String userSearchFilter;
    private final String groupBaseDN;
    private final String groupsNameAttribute;
    private final String groupsSearchMemberAttribute;
    private final Optional<String> groupsSearchFilter;

    @Inject
    public LdapChainGroupProvider(
            LdapClient ldapClient,
            LdapGroupProviderConfig config,
            LdapChainGroupProviderConfig chainConfig)
    {
        this.ldapClient = requireNonNull(ldapClient, "ldapClient is null");
        this.ldapAdminUser = config.getLdapAdminUser();
        this.ldapAdminPassword = config.getLdapAdminPassword();
        this.userBaseDN = config.getLdapUserBaseDN();
        this.userSearchFilter = config.getLdapUserSearchFilter();
        this.groupBaseDN = chainConfig.getLdapGroupBaseDN();
        this.groupsNameAttribute = config.getLdapGroupsNameAttribute();
        this.groupsSearchMemberAttribute = chainConfig.getLdapGroupsSearchMemberAttribute();
        this.groupsSearchFilter = chainConfig.getLdapGroupsSearchFilter();
    }

    @Override
    public Set<String> getGroups(String user)
    {
        try {
            // Find the user's DN first
            Optional<String> userDistinguishedName = findUserDN(user);
            if (userDistinguishedName.isEmpty()) {
                return ImmutableSet.of();
            }

            // Build the search filter using LDAP_MATCHING_RULE_IN_CHAIN
            String searchFilter = buildChainSearchFilter(userDistinguishedName.get());

            return ldapClient.executeLdapQuery(ldapAdminUser, ldapAdminPassword,
                    new LdapQuery.LdapQueryBuilder()
                            .withSearchBase(groupBaseDN)
                            .withAttributes(groupsNameAttribute)
                            .withSearchFilter(searchFilter)
                            .build(),
                    search -> {
                        ImmutableSet.Builder<String> groupsBuilder = ImmutableSet.builder();
                        while (search.hasMore()) {
                            SearchResult groupResult = search.next();
                            Attribute groupName = groupResult.getAttributes().get(groupsNameAttribute);
                            if (groupName == null) {
                                log.warn("The group object [%s] does not have group name attribute [%s]. Falling back on object full name.", 
                                        groupResult, groupsNameAttribute);
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
            log.error(e, "LDAP search for user [%s] nested groups failed", user);
            return ImmutableSet.of();
        }
    }

    private Optional<String> findUserDN(String user)
            throws NamingException
    {
        return ldapClient.executeLdapQuery(ldapAdminUser, ldapAdminPassword,
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

    private String buildChainSearchFilter(String userDN)
    {
        // Use LDAP_MATCHING_RULE_IN_CHAIN for recursive group membership
        // This is much more efficient than recursive queries
        String memberFilter = String.format("(%s:1.2.840.113556.1.4.1941:=%s)", 
                groupsSearchMemberAttribute, userDN);
        
        if (groupsSearchFilter.isPresent()) {
            // Combine with additional group filter if specified
            return String.format("(&(%s)(%s))", groupsSearchFilter.get(), memberFilter);
        }
        
        return memberFilter;
    }
}
