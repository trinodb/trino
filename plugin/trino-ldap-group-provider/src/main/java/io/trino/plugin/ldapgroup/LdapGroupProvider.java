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
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.GroupProvider;

import javax.naming.NamingException;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class LdapGroupProvider
        implements GroupProvider
{
    private static final Logger log = Logger.get(LdapGroupProvider.class);

    private final String userMemberOfAttribute;
    private final LdapGroupProviderClient ldapGroupClient;

    @Inject
    public LdapGroupProvider(LdapGroupProviderClient ldapGroupClient, LdapGroupProviderConfig config)
    {
        this.ldapGroupClient = requireNonNull(ldapGroupClient, "ldap client is null");
        this.userMemberOfAttribute = config.getLdapUserMemberOfAttribute();
    }

    @Override
    public Set<String> getGroups(String user)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            ImmutableSet.Builder<String> groupsBuilder = ImmutableSet.builder();
            Optional<LdapSearchUserResult> userSearchResultOpt;
            try {
                userSearchResultOpt = ldapGroupClient.getUser(user);
            }
            catch (NamingException e) {
                throw new RuntimeException(String.format("Error loading user %s from LDAP", user), e);
            }

            if (userSearchResultOpt.isEmpty()) {
                log.warn("User %s does not exist in LDAP server", user);
                return ImmutableSet.of();
            }

            LdapSearchUserResult userSearchResult = userSearchResultOpt.get();
            log.debug("Fetched user DN and search attributes from LDAP for user %s - %s", user, userSearchResult);

            groupsBuilder.addAll(userSearchResult
                    .userAttributes()
                    .entrySet()
                    .stream()
                    .filter(e -> !e.getKey().equalsIgnoreCase(userMemberOfAttribute))
                    .map(e -> e
                            .getValue()
                            .stream()
                            .map(v -> String.format("%s=%s", e.getKey(), v))
                            .toList())
                    .flatMap(List::stream)
                    .iterator());

            try {
                List<String> groups = ldapGroupClient.getGroups(userSearchResult);
                log.debug("Fetched LDAP groups for user %s - %s", user, groups);
                groupsBuilder.addAll(groups);
            }
            catch (NamingException e) {
                throw new RuntimeException(String.format("Error loading groups for user %s from LDAP", user), e);
            }

            Set<String> groups = groupsBuilder.build();
            log.debug("Loaded LDAP groups for user %s - %s", user, groups);
            return groups;
        }
    }
}
