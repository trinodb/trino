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
import io.airlift.log.Logger;
import io.trino.plugin.base.ldap.LdapClient;
import io.trino.plugin.base.ldap.LdapQuery;
import io.trino.plugin.base.ldap.LdapUtil;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.GroupProvider;

import javax.inject.Inject;
import javax.naming.NamingException;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCacheWithWeakInvalidateAll;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LdapGroupProvider
        implements GroupProvider
{
    private static final Logger log = Logger.get(LdapGroupProvider.class);
    private static final Pattern EXTRACT_GROUP_PATTERN = Pattern.compile("(?i)cn=([^,]*),(.*)$");

    private final LdapClient client;

    private final List<String> userBindSearchPatterns;
    private final Optional<String> userBaseDistinguishedName;
    private final String groupBaseDistinguishedName;
    private String groupSearchFilter;
    private final String bindDistinguishedName;
    private final String bindPassword;
    private final boolean allowUserNotExist;
    private final LdapUtil util;

    private final LoadingCache<String, Set<String>> groupCache;

    @Inject
    public LdapGroupProvider(LdapClient client, LdapGroupProviderConfig ldapConfig, LdapUtil util)
    {
        this.client = requireNonNull(client, "client is null");
        requireNonNull(ldapConfig, "ldapConfig is null");

        this.userBindSearchPatterns = ldapConfig.getUserBindSearchPatterns();
        this.userBaseDistinguishedName = Optional.ofNullable(ldapConfig.getUserBaseDistinguishedName());
        this.groupBaseDistinguishedName = ldapConfig.getGroupBaseDistinguishedName();
        this.bindDistinguishedName = ldapConfig.getBindDistinguishedName();
        this.bindPassword = ldapConfig.getBindPassword();
        this.allowUserNotExist = ldapConfig.isAllowUserNotExist();
        this.groupSearchFilter = ldapConfig.getGroupSearchFilter();
        this.util = util;
        this.groupCache = buildNonEvictableCacheWithWeakInvalidateAll(
                CacheBuilder.newBuilder()
                        .expireAfterWrite(ldapConfig.getLdapCacheTtl().toMillis(), MILLISECONDS),
                CacheLoader.from(this::getGroupsWithBindDistinguishedName));
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

        if (util.containsSpecialCharacters(user)) {
            throw new AccessDeniedException("Username contains a special LDAP character");
        }

        try {
            for (String userBindSearchPattern : userBindSearchPatterns) {
                String searchFilter = util.replaceUser(userBindSearchPattern, user);
                String userDistinguishedName = lookupUserDistinguishedName(
                        this.userBaseDistinguishedName.orElseThrow(),
                        searchFilter,
                        this.bindDistinguishedName,
                        this.bindPassword);
                String groupFilter = util.replaceUser(this.groupSearchFilter, userDistinguishedName);
                log.debug("Querying LDAP from %s using filter %s", this.groupBaseDistinguishedName, groupFilter);
                Set<String> queryResult = lookupUserDistinguishedNames(
                        this.groupBaseDistinguishedName, groupFilter,
                        bindDistinguishedName, bindPassword);
                groupSet = queryResult.stream().map(source -> {
                    try {
                        log.debug("extracting group from %s", source);
                        Matcher m = EXTRACT_GROUP_PATTERN.matcher(source);
                        if (m.find()) {
                            return m.group(1);
                        }
                        log.debug("Cannot extract group cn [%s]", source);
                        return "";
                    }
                    catch (Exception e) {
                        log.debug(e, "Unknown exception, %s", e.getMessage());
                        return "";
                    }
                }).filter(Objects::nonNull).collect(Collectors.toSet());
            }
            log.debug("Getting groups successful for user [%s]", user);
            log.debug("Groups: %s", groupSet.toString());
        }
        catch (AccessDeniedException e) {
            if (allowUserNotExist) {
                return Collections.emptySet();
            }
            else {
                log.debug(e, "Getting user failed [%s], %s", user, e.getMessage());
                throw e;
            }
        }
        catch (NamingException e) {
            log.debug(e, "Getting groups failed for user [%s], %s", user, e.getMessage());
            throw new RuntimeException(e);
        }
        return groupSet;
    }

    public Set<String> lookupUserDistinguishedNames(String searchBase, String searchFilter, String contextUserDistinguishedName, String contextPassword)
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

    String lookupUserDistinguishedName(String userBaseDistinguishedName, String groupAuthorizationSearchPattern, String bindDistinguishedName, String bindPassword)
            throws NamingException
    {
        Set<String> userDistinguishedNames = lookupUserDistinguishedNames(userBaseDistinguishedName, groupAuthorizationSearchPattern, bindDistinguishedName, bindPassword);
        if (userDistinguishedNames.isEmpty()) {
            String message = format("User [%s] not a member of an authorized group", userBaseDistinguishedName);
            log.debug("%s", message);
            throw new AccessDeniedException(message);
        }
        if (userDistinguishedNames.size() > 1) {
            String message = format("Multiple group membership results for user [%s]: %s", userBaseDistinguishedName, userDistinguishedNames);
            log.debug("%s", message);
            throw new AccessDeniedException(message);
        }
        return getOnlyElement(userDistinguishedNames);
    }
}
