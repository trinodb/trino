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

import io.airlift.log.Logger;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.GroupProvider;

import javax.inject.Inject;
import javax.naming.NamingException;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class LdapGroupProvider
        implements GroupProvider
{
    private static final Logger log = Logger.get(LdapGroupProvider.class);

    private final LdapClient client;

    private final List<String> userBindSearchPatterns;
    private final Optional<String> userBaseDistinguishedName;
    private final Optional<String> bindDistinguishedName;
    private final Optional<String> bindPassword;

    @Inject
    public LdapGroupProvider(LdapClient client, LdapConfig ldapConfig)
    {
        this.client = requireNonNull(client, "client is null");

        this.userBindSearchPatterns = ldapConfig.getUserBindSearchPatterns();
        this.userBaseDistinguishedName = Optional.ofNullable(ldapConfig.getUserBaseDistinguishedName());
        this.bindDistinguishedName = Optional.ofNullable(ldapConfig.getBindDistingushedName());
        this.bindPassword = Optional.ofNullable(ldapConfig.getBindPassword());

        checkArgument(
                 userBaseDistinguishedName.isPresent(),
                "Base distinguished name (DN) for user must be provided");
        checkArgument(
                bindDistinguishedName.isPresent() == bindPassword.isPresent(),
                "Both bind distinguished name and bind password must be provided");
        checkArgument(
                !userBindSearchPatterns.isEmpty(),
                "User bind search pattern must be provided");
    }

    @Override
    public Set<String> getGroups(String user)
    {
        if (LdapUtil.containsSpecialCharacters(user)) {
            throw new AccessDeniedException("Username contains a special LDAP character");
        }
        for (String userBindSearchPattern : userBindSearchPatterns) {
            String userDistinguishedName = LdapUtil.replaceUser(userBindSearchPattern, user);
            String searchBase = userBaseDistinguishedName.orElseThrow();
            try {
                return client.lookupUserGroups(searchBase, userDistinguishedName, bindDistinguishedName.orElseThrow(), bindPassword.orElseThrow());
            }
            catch (NamingException e) {
                log.debug(e, "Authentication failed for user [%s], %s", user, e.getMessage());
                throw new RuntimeException("Authentication error");
            }
        }
        return null;
    }
}
