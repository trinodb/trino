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
import io.trino.plugin.base.ldap.LdapClient;

import javax.naming.NamingException;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

final class RecursiveLdapGroupResolver
        extends BaseLdapGroupResolver
{
    @Inject
    public RecursiveLdapGroupResolver(
            LdapClient ldapClient,
            LdapGroupProviderConfig config,
            LdapFilteringGroupProviderConfig filteringConfig)
    {
        super(ldapClient, config, filteringConfig, String.format("%s={0}", filteringConfig.getLdapGroupsSearchMemberAttribute()));
    }

    @Override
    public Set<String> resolveGroups(String memberDistinguishedName)
    {
        Queue<String> membersToResolve = new ArrayDeque<>();
        membersToResolve.add(memberDistinguishedName);
        Set<String> visitedMembers = new HashSet<>();
        Set<String> groups = new HashSet<>();

        while (!membersToResolve.isEmpty()) {
            String currentMember = membersToResolve.remove();
            if (!visitedMembers.add(currentMember)) {
                continue;
            }

            try {
                Set<LdapGroup> groupsForMember = searchGroups(currentMember);
                groupsForMember.forEach(group -> {
                    groups.add(group.name());
                    membersToResolve.add(group.distinguishedName());
                });
            }
            catch (NamingException e) {
                log.error(e, "LDAP group search for member [%s] failed", currentMember);
                return ImmutableSet.of();
            }
        }

        return ImmutableSet.copyOf(groups);
    }
}
