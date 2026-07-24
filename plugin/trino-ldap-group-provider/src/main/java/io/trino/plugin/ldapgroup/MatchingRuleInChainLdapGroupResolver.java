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

import com.google.inject.Inject;
import io.trino.plugin.base.ldap.LdapClient;

final class MatchingRuleInChainLdapGroupResolver
        extends DirectLdapGroupResolver
{
    private static final String LDAP_MATCHING_RULE_IN_CHAIN = "1.2.840.113556.1.4.1941";

    @Inject
    public MatchingRuleInChainLdapGroupResolver(
            LdapClient ldapClient,
            LdapGroupProviderConfig config,
            LdapFilteringGroupProviderConfig filteringConfig)
    {
        super(ldapClient,
                config,
                filteringConfig,
                String.format("%s:%s:={0}", filteringConfig.getLdapGroupsSearchMemberAttribute(), LDAP_MATCHING_RULE_IN_CHAIN));
    }

    @Override
    protected String searchDescription()
    {
        return "LDAP_MATCHING_RULE_IN_CHAIN search";
    }
}
