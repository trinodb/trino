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

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

class DirectLdapGroupResolver
        extends BaseLdapGroupResolver
{
    @Inject
    public DirectLdapGroupResolver(
            LdapClient ldapClient,
            LdapGroupProviderConfig config,
            LdapFilteringGroupProviderConfig filteringConfig)
    {
        super(ldapClient, config, filteringConfig, String.format("%s={0}", filteringConfig.getLdapGroupsSearchMemberAttribute()));
    }

    protected DirectLdapGroupResolver(
            LdapClient ldapClient,
            LdapGroupProviderConfig config,
            LdapFilteringGroupProviderConfig filteringConfig,
            String groupSearchMemberPredicate)
    {
        super(ldapClient, config, filteringConfig, groupSearchMemberPredicate);
    }

    @Override
    public Set<String> resolveGroups(String memberDistinguishedName)
    {
        try {
            return searchGroups(memberDistinguishedName).stream()
                    .map(LdapGroup::name)
                    .collect(toImmutableSet());
        }
        catch (NamingException e) {
            log.error(e, "LDAP group search for member [%s] failed", memberDistinguishedName);
            return ImmutableSet.of();
        }
    }
}
