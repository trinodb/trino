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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.base.ldap.LdapClient;
import io.trino.plugin.base.ldap.LdapQuery;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.GroupProvider;

import javax.naming.InvalidNameException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.cache.SafeCaches.buildNonEvictableCacheWithWeakInvalidateAll;
import static io.trino.plugin.password.ldap.LdapAuthenticator.containsSpecialCharacters;
import static io.trino.plugin.password.ldap.LdapAuthenticator.replaceUser;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LdapGroupProvider
        implements GroupProvider
{
    private static final Logger log = Logger.get(LdapGroupProvider.class);
    private final LdapClient client;
    protected final List<String> userBindSearchPatterns;
    protected final Optional<String> userBaseDistinguishedName;
    protected final Optional<String> groupBaseDistinguishedName;
    protected final Optional<String> groupNameAttribute;
    protected final Optional<String> groupMembershipAttribute;
    protected final Optional<String> groupUserMembershipAttribute;
    protected final String bindDistinguishedName;
    protected final String bindPassword;
    protected final boolean allowUserNotExist;
    private final LoadingCache<String, Set<String>> groupCache;
    private Optional<LdapGroupProviderQuery> ldapGroupProviderQueryUser;
    private Optional<LdapGroupProviderQuery> ldapGroupProviderQueryGroup;

    @Inject
    public LdapGroupProvider(LdapClient client, LdapGroupProviderConfig ldapConfig)
    {
        this.client = requireNonNull(client, "client is null");
        requireNonNull(ldapConfig, "ldapConfig is null");

        this.userBindSearchPatterns = ldapConfig.getUserBindSearchPatterns();
        this.userBaseDistinguishedName = Optional.ofNullable(ldapConfig.getUserBaseDistinguishedName());
        this.groupBaseDistinguishedName = Optional.ofNullable(ldapConfig.getGroupBaseDistinguishedName());
        this.groupNameAttribute = Optional.ofNullable(ldapConfig.getGroupNameAttribute());
        this.bindDistinguishedName = ldapConfig.getBindDistinguishedName();
        this.bindPassword = ldapConfig.getBindPassword();
        this.allowUserNotExist = ldapConfig.isAllowUserNotExist();
        this.groupMembershipAttribute = Optional.ofNullable(ldapConfig.getGroupMembershipAttribute());
        this.groupUserMembershipAttribute = Optional.ofNullable(ldapConfig.getGroupUserMembershipAttribute());
        if (this.groupMembershipAttribute.isEmpty() && this.groupUserMembershipAttribute.isEmpty()) {
            throw new LdapGroupMembershipException("both ldap.group-membership-attribute and "
                    + "ldap.group-user-membership-attribute cannot be empty");
        }
        if (this.groupMembershipAttribute.isPresent() && this.groupNameAttribute.isEmpty()) {
            throw new LdapGroupMembershipException("ldap.group-membership-attribute is set "
                    + "ldap.group-name-attribute cannot be empty");
        }
        this.groupCache = buildNonEvictableCacheWithWeakInvalidateAll(
                CacheBuilder.newBuilder()
                        .expireAfterWrite(ldapConfig.getLdapCacheTtl().toMillis(), MILLISECONDS),
                CacheLoader.from(this::getGroupsWithBindDistinguishedName));
        if (this.groupUserMembershipAttribute.isPresent()) {
            this.ldapGroupProviderQueryUser = Optional.of(new LdapGroupProviderQueryUser());
        }
        else {
            this.ldapGroupProviderQueryUser = Optional.empty();
        }
        if (this.groupMembershipAttribute.isPresent()) {
            this.ldapGroupProviderQueryGroup = Optional.of(new LdapGroupProviderQueryGroup());
        }
        else {
            this.ldapGroupProviderQueryGroup = Optional.empty();
        }
    }

    @VisibleForTesting
    void invalidateCache()
    {
        groupCache.invalidateAll();
    }

    @Override
    public Set<String> getGroups(String user)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            return groupCache.getUnchecked(user);
        }
        catch (UncheckedExecutionException e) {
            throw e;
        }
    }

    private Set<String> getGroupsWithBindDistinguishedName(String user)
    {
        Set<String> groupSet = Collections.emptySet();

        if (containsSpecialCharacters(user)) {
            throw new AccessDeniedException("Username contains a special LDAP character");
        }
        // Query User first, Groups second
        for (String userBindSearchPattern : userBindSearchPatterns) {
            String userSearchFilter = replaceUser(userBindSearchPattern, user);
            if (this.ldapGroupProviderQueryUser.isPresent()) {
                groupSet = this.ldapGroupProviderQueryUser.get().getGroupsWithBindDistinguishedName(this, userSearchFilter);
                if (groupSet.size() > 0) {
                    log.debug("Getting groups successful using user RDN [%s]", user);
                    log.debug("Groups: %s", groupSet.toString());
                    return groupSet;
                }
            }
            if (this.ldapGroupProviderQueryGroup.isPresent()) {
                groupSet = this.ldapGroupProviderQueryGroup.get().getGroupsWithBindDistinguishedName(this, userSearchFilter);
                if (groupSet.size() > 0) {
                    log.debug("Getting groups successful with group query for user [%s]", user);
                    log.debug("Groups: %s", groupSet.toString());
                }
                else {
                    log.debug("No Groups found for user [%s]", user);
                }
            }
        }
        return groupSet;
    }

    public Set<String> lookupUserDistinguishedNames(String searchBase,
            String searchFilter,
            String contextUserDistinguishedName,
            String contextPassword)
            throws NamingException
    {
        return client.executeLdapQuery(
                contextUserDistinguishedName,
                contextPassword,
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(searchBase)
                        .withSearchFilter(searchFilter)
                        .build(),
                searchResults -> {
                    ImmutableSet.Builder<String> distinguishedNames = ImmutableSet.builder();
                    while (searchResults.hasMore()) {
                        distinguishedNames.add(searchResults.next().getNameInNamespace());
                    }
                    return distinguishedNames.build();
                });
    }

    public Set<String> lookupUserAttributes(String searchBase,
            String searchFilter,
            String attributes,
            String contextUserDistinguishedName,
            String contextPassword)
            throws NamingException
    {
        return client.executeLdapQuery(
                contextUserDistinguishedName,
                contextPassword,
                new LdapQuery.LdapQueryBuilder()
                        .withSearchBase(searchBase)
                        .withSearchFilter(searchFilter)
                        .withAttributes(attributes)
                        .build(),
                searchResults -> {
                    ImmutableSet.Builder<String> userAttributes = ImmutableSet.builder();
                    while (searchResults.hasMore()) {
                        final NamingEnumeration<? extends Attribute> attrs = searchResults.next().getAttributes().getAll();
                        while (attrs.hasMore()) {
                            final NamingEnumeration<?> avals = attrs.next().getAll();
                            while (avals.hasMore()) {
                                userAttributes.add(avals.next().toString());
                            }
                        }
                    }
                    return userAttributes.build();
                });
    }

    public String lookupUserDistinguishedName(String userBaseDistinguishedName,
            String searchFilter,
            String bindDistinguishedName,
            String bindPassword)
            throws NamingException
    {
        Set<String> userDistinguishedNames = lookupUserDistinguishedNames(userBaseDistinguishedName,
                searchFilter, bindDistinguishedName, bindPassword);
        if (userDistinguishedNames.isEmpty()) {
            if (allowUserNotExist) {
                return "";
            }
            else {
                String message = format("User filter [%s] failed for baseDN [%s]", searchFilter, userBaseDistinguishedName);
                log.debug("%s", message);
                throw new AccessDeniedException(message);
            }
        }
        if (userDistinguishedNames.size() > 1) {
            String message = format("Multiple group membership results for user [%s]: %s", userBaseDistinguishedName,
                    userDistinguishedNames);
            log.debug("%s", message);
            throw new AccessDeniedException(message);
        }
        return getOnlyElement(userDistinguishedNames);
    }

    static String extractGroupName(
            String source,
            String groupNameAttribute,
            String groupUserMembershipAttribute)
            throws NamingException
    {
        LdapName ldn;
        try {
            ldn = new LdapName(source);
        }
        catch (InvalidNameException e) {
            if (!source.isEmpty() && groupUserMembershipAttribute != null && groupUserMembershipAttribute.equalsIgnoreCase("memberOf")) {
                return source;
            }
            throw e;
        }
        List<Rdn> rdns = ldn.getRdns();
        if (rdns.isEmpty()) {
            throw new NamingException("ExtractGroupName - DN is empty");
        }
        Rdn rdn = rdns.get(rdns.size() - 1);
        if (rdn.getType().equalsIgnoreCase(groupNameAttribute)) {
            return (String) rdn.getValue();
        }
        throw new NamingException("ExtractGroupName - Unable to find a group name using a " +
                "group-name-attribute [" + groupNameAttribute + "]. The DN is [" + source + "]");
    }

    static Set<String> extractGroupNameFromUser(
            Set<String> userGroups,
            String groupNameAttribute,
            String groupUserMembershipAttribute)
    {
        Set<String> groupSet = userGroups.stream().map(source -> {
            log.debug("extracting group from %s", source);
            try {
                return extractGroupName(source, groupNameAttribute, groupUserMembershipAttribute);
            }
            catch (NamingException e) {
                log.debug("Cannot extract group [%s] [%s] [%s]", groupNameAttribute, groupUserMembershipAttribute, source);
            }
            return null;
        }).filter(Objects::nonNull).collect(Collectors.toUnmodifiableSet());
        return groupSet;
    }
}
