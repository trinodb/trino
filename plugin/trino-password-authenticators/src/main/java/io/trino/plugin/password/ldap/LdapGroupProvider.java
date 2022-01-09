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
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.GroupProvider;

import javax.inject.Inject;
import javax.naming.NamingException;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LdapGroupProvider
        implements GroupProvider
{
    private static final Logger log = Logger.get(LdapGroupProvider.class);
    private static final Pattern EXTRACT_GROUP_PATTERN = Pattern.compile("(?i)cn=([^,]*),(.*)$");
    static final String GROUP_SEARCH_FILTER_PREFIX = "member=";

    private final LdapClient client;
    private final LdapCommon ldapCommon;

    private final Optional<String> groupBaseDistinguishedName;
    private final Optional<String> bindDistinguishedName;
    private final Optional<String> bindPassword;
    private final boolean allowUserNotExist;

    private final LoadingCache<String, Set<String>> groupCache;

    @Inject
    public LdapGroupProvider(LdapClient client, LdapConfig ldapConfig, LdapCommon ldapCommon)
    {
        this.client = requireNonNull(client, "client is null");
        this.ldapCommon = ldapCommon;

        this.bindDistinguishedName = Optional.ofNullable(ldapConfig.getBindDistingushedName());
        this.bindPassword = Optional.ofNullable(ldapConfig.getBindPassword());
        this.groupBaseDistinguishedName = Optional.ofNullable(ldapConfig.getGroupBaseDistinguishedName());
        this.allowUserNotExist = ldapConfig.isAllowUserNotExist();
        Optional<String> userBaseDistinguishedName = Optional.ofNullable(ldapConfig.getUserBaseDistinguishedName());

        checkArgument(
                bindDistinguishedName.isPresent(),
                "Bind distinguished name must be provided");

        checkArgument(
                userBaseDistinguishedName.isPresent(),
                "Base distinguished name (DN) for user must be provided");

        checkArgument(
                groupBaseDistinguishedName.isPresent(),
                "Base distinguished name (DN) for group must be provided");

        this.groupCache = CacheBuilder.newBuilder()
                .expireAfterWrite(ldapConfig.getLdapCacheTtl().toMillis(), MILLISECONDS)
                .build(CacheLoader.from(this::getGroupsWithBindDistinguishedName));
    }

    @Override
    public Set<String> getGroups(String user)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            return groupCache.getUnchecked(user);
        }
        catch (UncheckedExecutionException e) {
            if (e.getCause() instanceof AccessDeniedException) {
                if (allowUserNotExist) {
                    return Collections.emptySet();
                }
                else {
                    throw (AccessDeniedException) e.getCause();
                }
            }
            throw e;
        }
    }

    @VisibleForTesting
    void invalidateCache()
    {
        groupCache.invalidateAll();
    }

    private Set<String> getGroupsWithBindDistinguishedName(String user)
    {
        Set<String> groupSet = Collections.emptySet();

        if (LdapCommon.containsSpecialCharacters(user)) {
            throw new AccessDeniedException("Username contains a special LDAP character");
        }

        try {
            String userDistinguishedName = ldapCommon.lookupUserDistinguishedName(user);
            Set<String> queryResult = client.lookupUserDistinguishedNames(
                    groupBaseDistinguishedName.orElseThrow(), GROUP_SEARCH_FILTER_PREFIX + userDistinguishedName,
                    bindDistinguishedName.orElseThrow(), bindPassword.orElseThrow());
            groupSet = queryResult.stream().map(source -> {
                try {
                    Matcher m = EXTRACT_GROUP_PATTERN.matcher(source);
                    if (m.find()) {
                        return m.group(1);
                    }
                    else {
                        log.debug("Cannot extract group cn [%s]", source);
                        return null;
                    }
                }
                catch (Exception e) {
                    log.debug(e, "Unknown exception, %s", e.getMessage());
                    return null;
                }
            }).filter(Objects::nonNull).collect(Collectors.toSet());
            log.debug("Getting groups successful for user [%s]", user);
            log.debug("Groups: %s", groupSet.toString());
        }
        catch (AccessDeniedException e) {
            log.debug(e, "Getting user failed [%s], %s", user, e.getMessage());
            throw e;
        }
        catch (NamingException e) {
            log.debug(e, "Getting groups failed for user [%s], %s", user, e.getMessage());
            throw new RuntimeException(e);
        }

        return groupSet;
    }
}
