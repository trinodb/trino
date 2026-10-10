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

import javax.naming.NamingException;

import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.ldapgroup.LdapFilteringGroupProviderConfig.LdapGroupSearchMode.MATCHING_RULE_IN_CHAIN;
import static java.util.Objects.requireNonNull;

final class MatchingRuleInChainLdapGroupResolver
        implements LdapGroupResolver
{
    private static final String LDAP_MATCHING_RULE_IN_CHAIN = "1.2.840.113556.1.4.1941";
    private static final Logger log = Logger.get(MatchingRuleInChainLdapGroupResolver.class);

    private final LdapGroupSearcher groupSearcher;
    private final String groupSearchMemberPredicate;
    private final Optional<String> groupSearchFilter;

    @Inject
    public MatchingRuleInChainLdapGroupResolver(
            LdapGroupSearcher groupSearcher,
            LdapFilteringGroupProviderConfig filteringConfig)
    {
        this.groupSearcher = requireNonNull(groupSearcher, "groupSearcher is null");
        this.groupSearchMemberPredicate = String.format("%s:%s:={0}", filteringConfig.getLdapGroupsSearchMemberAttribute(), LDAP_MATCHING_RULE_IN_CHAIN);
        this.groupSearchFilter = filteringConfig.getLdapGroupsSearchFilter();
    }

    @Override
    public Set<LdapGroup> resolveGroups(String memberDistinguishedName)
    {
        try {
            return groupSearcher.searchGroups(memberDistinguishedName, groupSearchMemberPredicate, groupSearchFilter, MATCHING_RULE_IN_CHAIN);
        }
        catch (NamingException e) {
            log.error(e, "LDAP group search for member [%s] failed", memberDistinguishedName);
            return ImmutableSet.of();
        }
    }
}
