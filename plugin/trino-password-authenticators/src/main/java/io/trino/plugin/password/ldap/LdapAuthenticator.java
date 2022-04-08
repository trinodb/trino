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
import com.google.common.base.CharMatcher;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.trino.collect.cache.NonKeyEvictableLoadingCache;
import io.trino.plugin.password.Credential;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.BasicPrincipal;
import io.trino.spi.security.PasswordAuthenticator;

import javax.inject.Inject;
import javax.naming.NamingException;

import java.security.Principal;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.collect.cache.SafeCaches.buildNonEvictableCacheWithWeakInvalidateAll;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LdapAuthenticator
        implements PasswordAuthenticator
{
    private static final Logger log = Logger.get(LdapAuthenticator.class);
    private static final CharMatcher SPECIAL_CHARACTERS = CharMatcher.anyOf(",=+<>#;*()\"\\\u0000");
    private static final CharMatcher WHITESPACE = CharMatcher.anyOf(" \r");

    private final LdapAuthenticatorClient client;

    private final List<String> userBindSearchPatterns;
    private final Optional<String> groupAuthorizationSearchPattern;
    private final Optional<String> userBaseDistinguishedName;
    private final Optional<String> bindDistinguishedName;
    private final Optional<String> bindPassword;

    private final NonKeyEvictableLoadingCache<Credential, Principal> authenticationCache;

    @Inject
    public LdapAuthenticator(LdapAuthenticatorClient client, LdapAuthenticatorConfig ldapAuthenticatorConfig)
    {
        this.client = requireNonNull(client, "client is null");

        this.userBindSearchPatterns = ldapAuthenticatorConfig.getUserBindSearchPatterns();
        this.groupAuthorizationSearchPattern = Optional.ofNullable(ldapAuthenticatorConfig.getGroupAuthorizationSearchPattern());
        this.userBaseDistinguishedName = Optional.ofNullable(ldapAuthenticatorConfig.getUserBaseDistinguishedName());
        this.bindDistinguishedName = Optional.ofNullable(ldapAuthenticatorConfig.getBindDistingushedName());
        this.bindPassword = Optional.ofNullable(ldapAuthenticatorConfig.getBindPassword());

        checkArgument(
                groupAuthorizationSearchPattern.isEmpty() || userBaseDistinguishedName.isPresent(),
                "Base distinguished name (DN) for user must be provided");
        checkArgument(
                bindDistinguishedName.isPresent() == bindPassword.isPresent(),
                "Both bind distinguished name and bind password must be provided together");
        checkArgument(
                bindDistinguishedName.isEmpty() || groupAuthorizationSearchPattern.isPresent(),
                "Group authorization search pattern must be provided when bind distinguished name is used");
        checkArgument(
                bindDistinguishedName.isPresent() || !userBindSearchPatterns.isEmpty(),
                "Either user bind search pattern or bind distinguished name must be provided");

        this.authenticationCache = buildNonEvictableCacheWithWeakInvalidateAll(
                CacheBuilder.newBuilder()
                        .expireAfterWrite(ldapAuthenticatorConfig.getLdapCacheTtl().toMillis(), MILLISECONDS),
                CacheLoader.from(bindDistinguishedName.isPresent()
                        ? this::authenticateWithBindDistinguishedName
                        : this::authenticateWithUserBind));
    }

    @VisibleForTesting
    void invalidateCache()
    {
        // Note: this may not invalidate ongoing loads (https://github.com/trinodb/trino/issues/10512, https://github.com/google/guava/issues/1881)
        // This is acceptable, since this operation is invoked in tests only, and not relied upon for correctness.
        authenticationCache.invalidateAll();
    }

    @Override
    public Principal createAuthenticatedPrincipal(String user, String password)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            return authenticationCache.getUnchecked(new Credential(user, password));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), AccessDeniedException.class);
            throw e;
        }
    }

    private Principal authenticateWithUserBind(Credential credential)
    {
        String user = credential.getUser();
        if (containsSpecialCharacters(user)) {
            throw new AccessDeniedException("Username contains a special LDAP character");
        }
        Exception lastException = new RuntimeException();
        for (String userBindSearchPattern : userBindSearchPatterns) {
            try {
                String userDistinguishedName = replaceUser(userBindSearchPattern, user);
                if (groupAuthorizationSearchPattern.isPresent()) {
                    // user password is also validated as user DN and password is used for querying LDAP
                    String searchBase = userBaseDistinguishedName.orElseThrow();
                    String groupSearch = replaceUser(groupAuthorizationSearchPattern.get(), user);
                    if (!client.isGroupMember(searchBase, groupSearch, userDistinguishedName, credential.getPassword())) {
                        String message = format("User [%s] not a member of an authorized group", user);
                        log.debug("%s", message);
                        throw new AccessDeniedException(message);
                    }
                }
                else {
                    client.validatePassword(userDistinguishedName, credential.getPassword());
                }
                log.debug("Authentication successful for user [%s]", user);
                return new BasicPrincipal(user);
            }
            catch (NamingException | AccessDeniedException e) {
                lastException = e;
            }
        }
        log.debug(lastException, "Authentication failed for user [%s], %s", user, lastException.getMessage());
        if (lastException instanceof AccessDeniedException) {
            throw (AccessDeniedException) lastException;
        }
        throw new RuntimeException("Authentication error");
    }

    private Principal authenticateWithBindDistinguishedName(Credential credential)
    {
        String user = credential.getUser();
        if (containsSpecialCharacters(user)) {
            throw new AccessDeniedException("Username contains a special LDAP character");
        }
        try {
            String userDistinguishedName = lookupUserDistinguishedName(user);
            client.validatePassword(userDistinguishedName, credential.getPassword());
            log.debug("Authentication successful for user [%s]", user);
        }
        catch (NamingException e) {
            log.debug(e, "Authentication failed for user [%s], %s", user, e.getMessage());
            throw new RuntimeException("Authentication error");
        }
        return new BasicPrincipal(credential.getUser());
    }

    /**
     * Returns {@code true} when parameter contains a character that has a special meaning in
     * LDAP search or bind name (DN).
     * <p>
     * Based on <a href="https://www.owasp.org/index.php/Preventing_LDAP_Injection_in_Java">Preventing_LDAP_Injection_in_Java</a> and
     * {@link javax.naming.ldap.Rdn#escapeValue(Object) escapeValue} method.
     */
    @VisibleForTesting
    static boolean containsSpecialCharacters(String user)
    {
        if (WHITESPACE.indexIn(user) == 0 || WHITESPACE.lastIndexIn(user) == user.length() - 1) {
            return true;
        }
        return SPECIAL_CHARACTERS.matchesAnyOf(user);
    }

    private String lookupUserDistinguishedName(String user)
            throws NamingException
    {
        String searchBase = userBaseDistinguishedName.orElseThrow();
        String searchFilter = replaceUser(groupAuthorizationSearchPattern.orElseThrow(), user);
        Set<String> userDistinguishedNames = client.lookupUserDistinguishedNames(searchBase, searchFilter, bindDistinguishedName.orElseThrow(), bindPassword.orElseThrow());
        if (userDistinguishedNames.isEmpty()) {
            String message = format("User [%s] not a member of an authorized group", user);
            log.debug("%s", message);
            throw new AccessDeniedException(message);
        }
        if (userDistinguishedNames.size() > 1) {
            String message = format("Multiple group membership results for user [%s]: %s", user, userDistinguishedNames);
            log.debug("%s", message);
            throw new AccessDeniedException(message);
        }
        return getOnlyElement(userDistinguishedNames);
    }

    private static String replaceUser(String pattern, String user)
    {
        return pattern.replace("${USER}", user);
    }
}
