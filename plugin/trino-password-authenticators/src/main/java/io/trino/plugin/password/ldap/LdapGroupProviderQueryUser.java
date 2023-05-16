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
import java.util.Set;

import static io.trino.plugin.password.ldap.LdapGroupProvider.extractGroupNameFromUser;

public class LdapGroupProviderQueryUser
        implements LdapGroupProviderQuery
{
    private static final Logger log = Logger.get(LdapGroupProviderQuery.class);

    @Override
    public Set<String> getGroupsWithBindDistinguishedName(LdapGroupProvider ldapGroupProvider, String userSearchFilter)
    {
        Set<String> groupSet = Collections.emptySet();
        try {
            if (ldapGroupProvider.groupUserMembershipAttribute.isPresent()) {
                try {
                    groupSet = ldapGroupProvider.lookupUserAttributes(
                            ldapGroupProvider.userBaseDistinguishedName.orElseThrow(),
                            userSearchFilter,
                            ldapGroupProvider.groupUserMembershipAttribute.get(),
                            ldapGroupProvider.bindDistinguishedName,
                            ldapGroupProvider.bindPassword);
                    groupSet = extractGroupNameFromUser(
                            groupSet,
                            ldapGroupProvider.groupNameAttribute.orElse(null),
                            ldapGroupProvider.groupUserMembershipAttribute.orElse(null));
                }
                catch (NamingException e) {
                    log.debug("Cannot extract group from user using attribute [%s] and filter [%s], continuing", ldapGroupProvider.groupNameAttribute.orElse(""), userSearchFilter);
                }
                if (groupSet.size() > 0) {
                    log.debug("Getting groups successful using user RDN [%s]", userSearchFilter);
                    log.debug("Groups: %s", groupSet.toString());
                    return groupSet;
                }
            }
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
        return groupSet;
    }
}
