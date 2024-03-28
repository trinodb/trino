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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestLdapGroupProviderFilteringClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LdapGroupProviderFilteringClientConfig.class)
                .setLdapGroupBaseDN(null)
                .setLdapGroupsSearchFilter(null)
                .setLdapGroupsSearchMemberAttribute("member"));
    }

    @Test
    public void testExplicitPropertyMapping()
    {
        Map<String, String> properties = ImmutableMap.of(
                "ldap.group-base-dn", "ou=group,dc=trino,dc=io",
                "ldap.group-search-filter", "(cn=dev*)",
                "ldap.group-search-member-attribute", "memberUser");

        LdapGroupProviderFilteringClientConfig expected = new LdapGroupProviderFilteringClientConfig()
                .setLdapGroupBaseDN("ou=group,dc=trino,dc=io")
                .setLdapGroupsSearchFilter("(cn=dev*)")
                .setLdapGroupsSearchMemberAttribute("memberUser");

        assertFullMapping(properties, expected);
    }
}
