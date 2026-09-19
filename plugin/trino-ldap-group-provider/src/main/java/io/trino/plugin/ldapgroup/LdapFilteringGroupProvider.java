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
import javax.naming.directory.SearchResult;

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class LdapFilteringGroupProvider
        implements GroupProvider
{
    private static final Logger log = Logger.get(LdapFilteringGroupProvider.class);

    private final LdapClient ldapClient;
    private final LdapGroupResolver ldapGroupResolver;
    private final String ldapAdminUser;
    private final String ldapAdminPassword;
    private final String userBaseDN;
    private final String userSearchFilter;

    @Inject
    public LdapFilteringGroupProvider(
            LdapClient ldapClient,
            LdapGroupResolver ldapGroupResolver,
            LdapGroupProviderConfig config)
    {
        this.ldapClient = requireNonNull(ldapClient, "ldapClient is null");
        this.ldapGroupResolver = requireNonNull(ldapGroupResolver, "ldapGroupResolver is null");
        this.ldapAdminUser = config.getLdapAdminUser();
        this.ldapAdminPassword = config.getLdapAdminPassword();
        this.userBaseDN = config.getLdapUserBaseDN();
        this.userSearchFilter = config.getLdapUserSearchFilter();
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
            userDistinguishedName = ldapClient.executeLdapQuery(
                    ldapAdminUser,
                    ldapAdminPassword,
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
                .map(ldapGroupResolver::resolveGroups)
                .orElse(ImmutableSet.of());
    }
}
