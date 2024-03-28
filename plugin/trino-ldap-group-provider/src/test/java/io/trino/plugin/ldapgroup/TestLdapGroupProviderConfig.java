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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

class TestLdapGroupProviderConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LdapGroupProviderConfig.class)
                .setLdapAdminUser(null)
                .setLdapAdminPassword(null)
                .setLdapUserBaseDN(null)
                .setLdapUserSearchFilter("(uid={0})")
                .setLdapUserSearchAttributes("")
                .setLdapGroupsNameAttribute("cn")
                .setLdapUserMemberOfAttribute(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = Map.of(
                "ldap.admin-user", "cn=admin,dc=trino,dc=io",
                "ldap.admin-password", "admin",
                "ldap.user-base-dn", "dc=trino,dc=io",
                "ldap.user-search-filter", "(accountName={0})",
                "ldap.user-search-attributes", "uuid,email",
                "ldap.group-name-attribute", "groupName",
                "ldap.user-member-of-attribute", "memberOf");

        LdapGroupProviderConfig expected = new LdapGroupProviderConfig()
                .setLdapAdminUser("cn=admin,dc=trino,dc=io")
                .setLdapAdminPassword("admin")
                .setLdapUserBaseDN("dc=trino,dc=io")
                .setLdapUserSearchFilter("(accountName={0})")
                .setLdapUserSearchAttributes("uuid,email")
                .setLdapGroupsNameAttribute("groupName")
                .setLdapUserMemberOfAttribute("memberOf");

        assertFullMapping(properties, expected);
    }
}
