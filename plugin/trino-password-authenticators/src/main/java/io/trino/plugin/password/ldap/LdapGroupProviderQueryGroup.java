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

import javax.naming.NamingException;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.plugin.password.ldap.LdapAuthenticator.replaceUser;
import static io.trino.plugin.password.ldap.LdapGroupProvider.extractGroupName;

public class LdapGroupProviderQueryGroup
        implements LdapGroupProviderQuery
{
    private static final Logger log = Logger.get(LdapGroupProviderQueryGroup.class);

    @Override
    public Set<String> getGroupsWithBindDistinguishedName(LdapGroupProvider ldapGroupProvider, String userSearchFilter)
    {
        Set<String> groupSet = Collections.emptySet();
        try {
            String userDistinguishedName = ldapGroupProvider.lookupUserDistinguishedName(
                    ldapGroupProvider.userBaseDistinguishedName.orElseThrow(),
                    userSearchFilter,
                    ldapGroupProvider.bindDistinguishedName,
                    ldapGroupProvider.bindPassword);
            String groupFilter = replaceUser(ldapGroupProvider.groupMembershipAttribute.get() + "=${USER}", userDistinguishedName);
            log.debug("Querying LDAP group from base [%s] using filter [%s]", ldapGroupProvider.groupBaseDistinguishedName, groupFilter);
            Set<String> queryResult = ldapGroupProvider.lookupUserDistinguishedNames(
                    ldapGroupProvider.groupBaseDistinguishedName.get(), groupFilter,
                    ldapGroupProvider.bindDistinguishedName, ldapGroupProvider.bindPassword);
            groupSet = queryResult.stream().map(source -> {
                log.debug("extracting group from [%s]", source);
                try {
                    return extractGroupName(
                            source,
                            ldapGroupProvider.groupNameAttribute.orElse(null),
                            ldapGroupProvider.groupUserMembershipAttribute.orElse(null));
                }
                catch (NamingException e) {
                    log.debug("Cannot extract group using attribute [%s] from [%s]", ldapGroupProvider.groupNameAttribute, source);
                    throw new RuntimeException(e);
                }
            }).filter(Objects::nonNull).collect(Collectors.toUnmodifiableSet());
        }
        catch (AccessDeniedException e) {
            if (ldapGroupProvider.allowUserNotExist) {
                return Collections.emptySet();
            }
            else {
                log.debug(e, "Getting user failed [%s], %s", userSearchFilter, e.getMessage());
                throw e;
            }
        }
        catch (NamingException e) {
            log.debug(e, "Getting groups failed for user [%s], %s", userSearchFilter, e.getMessage());
            throw new RuntimeException(e);
        }
        return groupSet;
    }
}
