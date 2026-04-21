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
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.base.ldap.LdapClient;
import io.trino.plugin.base.ldap.LdapQuery;
import io.trino.spi.security.GroupProvider;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class LdapSingleQueryGroupProvider
        implements GroupProvider
{
    private static final Logger log = Logger.get(LdapFilteringGroupProvider.class);

    private final LdapClient ldapClient;
    private final String ldapAdminUser;
    private final String ldapAdminPassword;
    private final String userBaseDN;
    private final String groupsNameAttribute;
    private final String userSearchFilter;
    private final String userMemberOfAttribute;

    @Inject
    public LdapSingleQueryGroupProvider(LdapClient ldapClient, LdapGroupProviderConfig config, LdapSingleQueryGroupProviderConfig singleQueryGroupProviderConfig)
    {
        this.ldapClient = requireNonNull(ldapClient, "ldapClient is null");
        this.ldapAdminUser = config.getLdapAdminUser();
        this.ldapAdminPassword = config.getLdapAdminPassword();
        this.userBaseDN = config.getLdapUserBaseDN();
        this.groupsNameAttribute = config.getLdapGroupsNameAttribute();
        this.userSearchFilter = config.getLdapUserSearchFilter();
        this.userMemberOfAttribute = singleQueryGroupProviderConfig.getLdapUserMemberOfAttribute();
    }

    /**
     * Perform an LDAP search for a user, retrieving the DN and the value of
     * {@link LdapSingleQueryGroupProviderConfig#getLdapUserMemberOfAttribute()}.
     * If multiple users match the search, the first one is used.
     * If the requested group attribute is missing from the LDAP document, it will be ignored
     * (i.e., the function will not error).
     * Swallows all LDAP exceptions.
     *
     * @param user Username of user, used with filter expression {@link LdapGroupProviderConfig#getLdapUserSearchFilter()}.
     * @return The relative domain name of all the values of attribute
     *         {@link LdapSingleQueryGroupProviderConfig#getLdapUserMemberOfAttribute()}.
     */
    @Override
    public Set<String> getGroups(String user)
    {
        try {
            return ldapClient.executeLdapQuery(ldapAdminUser, ldapAdminPassword,
                    new LdapQuery.LdapQueryBuilder()
                            .withSearchBase(userBaseDN)
                            .withAttributes(userMemberOfAttribute)
                            .withSearchFilter(userSearchFilter)
                            .withFilterArguments(user)
                            .build(),
                    search -> {
                        if (!search.hasMore()) {
                            log.debug("LDAP search for user [%s] using filter pattern [%s] found no matches", user, userSearchFilter);
                            return ImmutableSet.of();
                        }
                        SearchResult result = search.next();
                        Attribute attribute = result.getAttributes().get(userMemberOfAttribute);
                        if (attribute == null) {
                            log.warn("User [%s] does not have the memberOf attribute [%s]", user, userMemberOfAttribute);
                            return ImmutableSet.of();
                        }
                        return Streams.stream(attribute.getAll().asIterator())
                                .map(Object::toString)
                                .map(this::getGroupRelativeDomainName)
                                .flatMap(Optional::stream)
                                .collect(toImmutableSet());
                    });
        }
        catch (NamingException e) {
            log.error(e, "LDAP search for user [%s] failed", user);
            return ImmutableSet.of();
        }
    }

    private Optional<String> getGroupRelativeDomainName(String groupDistinguishedName)
    {
        LdapName groupLdapName;
        try {
            groupLdapName = new LdapName(groupDistinguishedName);
        }
        catch (NamingException namingException) {
            log.warn(namingException, "Group has malformed DN [%s]. Ignoring group.", groupDistinguishedName);
            return Optional.empty();
        }
        List<Rdn> groupRelativeDomainNames = groupLdapName.getRdns();
        if (groupRelativeDomainNames.isEmpty()) {
            log.warn("Group with DN [%s] has no Relative Domain Names. Ignoring group.", groupDistinguishedName);
            return Optional.empty();
        }
        Rdn lastRelativeDomainName = groupRelativeDomainNames.getLast();
        if (!lastRelativeDomainName.getType().equalsIgnoreCase(groupsNameAttribute)) {
            log.warn("Last Relative Domain Name of group with DN [%s] has the wrong type, expecting [%s], actual [%s]. Using DN.",
                    groupDistinguishedName, groupsNameAttribute, lastRelativeDomainName.getType());
            return Optional.of(groupDistinguishedName);
        }
        return Optional.of(lastRelativeDomainName.getValue().toString());
    }
}
