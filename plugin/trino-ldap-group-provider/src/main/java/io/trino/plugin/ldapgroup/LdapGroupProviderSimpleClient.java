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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.base.ldap.LdapClient;
import io.trino.plugin.base.ldap.LdapQuery;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class LdapGroupProviderSimpleClient
        implements LdapGroupProviderClient
{
    private static final Logger log = Logger.get(LdapGroupProviderFilteringClient.class);

    private final LdapClient ldapClient;
    private final String ldapAdminUser;
    private final String ldapAdminPassword;
    private final String userBaseDN;
    private final String groupsNameAttribute;
    private final String userSearchFilter;
    private final String userMemberOfAttribute;
    private final Set<String> userSearchAttributes;
    private final String[] userSearchAttributesArray;

    @Inject
    public LdapGroupProviderSimpleClient(LdapClient ldapClient, LdapGroupProviderConfig config)
    {
        this.ldapClient = requireNonNull(ldapClient, "ldap client is null");
        this.ldapAdminUser = config.getLdapAdminUser();
        this.ldapAdminPassword = config.getLdapAdminPassword();
        this.userBaseDN = config.getLdapUserBaseDN();
        this.groupsNameAttribute = config.getLdapGroupsNameAttribute();
        this.userSearchFilter = config.getLdapUserSearchFilter();
        this.userMemberOfAttribute = config.getLdapUserMemberOfAttribute();
        ImmutableSet.Builder<String> userAttributesBuilder = ImmutableSet.<String>builder().addAll(config.getLdapUserSearchAttributes());
        Optional.ofNullable(config.getLdapUserMemberOfAttribute()).ifPresent(userAttributesBuilder::add);
        this.userSearchAttributes = userAttributesBuilder.build();
        this.userSearchAttributesArray = userSearchAttributes.toArray(String[]::new);
    }

    /**
     * Perform an LDAP search for user, getting the DN and specified attributes {@link LdapGroupProviderConfig#getLdapUserSearchAttributes()}.
     * If multiple users are matched by the search the first one is used.
     * If requested attributes are missing from the LDAP document the attributes will be ignored (i.e. the function will
     * not error).
     *
     * @param user Username of user, used with filter expression {@link LdapGroupProviderConfig#getLdapUserSearchFilter()}.
     * @return An empty {@link Optional} if no users are matched. Otherwise, a {@link LdapSearchUserResult} object with
     * the DN of the user and the values of user search attributes loaded into a Map. The Map values are always
     * non-empty lists since requested attributes missing from the LDAP document are skipped.
     * @throws NamingException Underlying LDAP errors.
     */
    @Override
    public Optional<LdapSearchUserResult> getUser(String user)
            throws NamingException
    {
        return ldapClient.executeLdapQuery(ldapAdminUser, ldapAdminPassword,
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(userBaseDN)
                        .withAttributes(userSearchAttributesArray)
                        .withSearchFilter(userSearchFilter)
                        .withFilterArguments(user)
                        .build(),
                search -> {
                    if (!search.hasMore()) {
                        log.debug("LDAP search for user [%s] using filter pattern [%s] found no matches", user, userSearchFilter);
                        return Optional.empty();
                    }
                    SearchResult result = search.next();
                    ImmutableMap.Builder<String, List<String>> resAttributesBuilder = ImmutableMap.builder();
                    for (String attributeName : userSearchAttributes) {
                        Attribute attribute = result.getAttributes().get(attributeName);
                        if (attribute == null) {
                            log.warn("User [%s] does not have user search attribute [%s]", user, attributeName);
                            continue;
                        }
                        List<String> values = Streams.stream(attribute.getAll().asIterator())
                                .map(Object::toString)
                                .collect(toImmutableList());
                        if (values.isEmpty()) {
                            log.debug("User [%s] has attribute [%s] with not values", user, attributeName);
                            continue;
                        }
                        resAttributesBuilder.put(attributeName, values);
                    }
                    return Optional.of(new LdapSearchUserResult(result.getNameInNamespace(), resAttributesBuilder.buildOrThrow()));
                });
    }

    /**
     * Get user groups from values of memberOfAttribute.
     *
     * @param searchUserResult User search result from {@link LdapGroupProviderClient#getUser}.
     * @return A list of user groups.
     */
    @Override
    public List<String> getGroups(LdapSearchUserResult searchUserResult)
    {
        return searchUserResult
            .userAttributes()
            .getOrDefault(userMemberOfAttribute, ImmutableList.of())
            .stream()
            .map(this::getGroupRelativeDN)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(toImmutableList());
    }

    private Optional<String> getGroupRelativeDN(String groupDN)
    {
        LdapName groupLdapName;
        try {
            groupLdapName = new LdapName(groupDN);
        }
        catch (NamingException namingException) {
            log.warn(namingException, "Group has malformed DN [%s]. Ignoring group.", groupDN);
            return Optional.empty();
        }
        List<Rdn> groupRDNs = groupLdapName.getRdns();
        if (groupRDNs.isEmpty()) {
            log.warn("Group with DN [%s] has no Relative Domain Names. Ignoring group.", groupDN);
            return Optional.empty();
        }
        Rdn lastRDN = groupRDNs.getLast();
        if (!lastRDN.getType().equalsIgnoreCase(groupsNameAttribute)) {
            log.warn("Last Relative Domain Name of group with DN [%s] has the wrong type, expecting [%s], actual [%s]. Using DN.",
                    groupDN, groupsNameAttribute, lastRDN.getType());
            return Optional.of(groupDN);
        }
        return Optional.of(lastRDN.getValue().toString());
    }
}
