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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class LdapAuthenticator
        implements PasswordAuthenticator
{
    private static final Logger log = Logger.get(LdapAuthenticator.class);

    private final LdapClient client;
    private final LdapCommon ldapCommon;

    private final List<String> userBindSearchPatterns;
    private final Optional<String> groupAuthorizationSearchPattern;
    private final Optional<String> userBaseDistinguishedName;

    private final LoadingCache<Credential, Principal> authenticationCache;

    @Inject
    public LdapAuthenticator(LdapClient client, LdapConfig ldapConfig, LdapCommon ldapCommon)
    {
        this.client = requireNonNull(client, "client is null");
        this.ldapCommon = ldapCommon;

        this.userBindSearchPatterns = ldapConfig.getUserBindSearchPatterns();
        this.groupAuthorizationSearchPattern = Optional.ofNullable(ldapConfig.getGroupAuthorizationSearchPattern());
        this.userBaseDistinguishedName = Optional.ofNullable(ldapConfig.getUserBaseDistinguishedName());
        Optional<String> bindDistinguishedName = Optional.ofNullable(ldapConfig.getBindDistingushedName());

        checkArgument(
                bindDistinguishedName.isPresent() || !userBindSearchPatterns.isEmpty(),
                "Either user bind search pattern or bind distinguished name must be provided");

        this.authenticationCache = CacheBuilder.newBuilder()
                .expireAfterWrite(ldapConfig.getLdapCacheTtl().toMillis(), MILLISECONDS)
                .build(CacheLoader.from(bindDistinguishedName.isPresent()
                        ? this::authenticateWithBindDistinguishedName
                        : this::authenticateWithUserBind));
    }

    @VisibleForTesting
    void invalidateCache()
    {
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
        if (LdapCommon.containsSpecialCharacters(user)) {
            throw new AccessDeniedException("Username contains a special LDAP character");
        }
        Exception lastException = new RuntimeException();
        for (String userBindSearchPattern : userBindSearchPatterns) {
            try {
                String userDistinguishedName = LdapCommon.replaceUser(userBindSearchPattern, user);
                if (groupAuthorizationSearchPattern.isPresent()) {
                    // user password is also validated as user DN and password is used for querying LDAP
                    String searchBase = userBaseDistinguishedName.orElseThrow();
                    String groupSearch = LdapCommon.replaceUser(groupAuthorizationSearchPattern.get(), user);
                    if (!client.isGroupMember(searchBase, groupSearch, userDistinguishedName, credential.getPassword())) {
                        String message = format("User [%s] not a member of an authorized group or not exist", user);
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
        if (LdapCommon.containsSpecialCharacters(user)) {
            throw new AccessDeniedException("Username contains a special LDAP character");
        }
        try {
            String userDistinguishedName = ldapCommon.lookupUserDistinguishedName(user);
            client.validatePassword(userDistinguishedName, credential.getPassword());
            log.debug("Authentication successful for user [%s]", user);
        }
        catch (NamingException e) {
            log.debug(e, "Authentication failed for user [%s], %s", user, e.getMessage());
            throw new RuntimeException("Authentication error");
        }
        return new BasicPrincipal(credential.getUser());
    }
}
