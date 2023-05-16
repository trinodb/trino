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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertValidates;

public class TestLdapGroupProviderConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LdapGroupProviderConfig.class)
                .setUserBindSearchPatterns(" : ")
                .setBindDistinguishedName(null)
                .setUserBaseDistinguishedName(null)
                .setGroupNameAttribute(null)
                .setBindPassword(null)
                .setAllowUserNotExist(false)
                .setGroupBaseDistinguishedName(null)
                .setGroupMembershipAttribute(null)
                .setGroupUserMembershipAttribute(null)
                .setLdapCacheTtl(new Duration(1, TimeUnit.HOURS)));
    }

    @Test
    public void testExplicitConfig()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("ldap.user-bind-pattern", "uid=${USER},ou=org,dc=test,dc=com:uid=${USER},ou=alt")
                .put("ldap.user-base-dn", "dc=test,dc=com")
                .put("ldap.group-name-attribute", "cn")
                .put("ldap.bind-dn", "CN=User Name,OU=CITY_OU,OU=STATE_OU,DC=domain,DC=domain_root")
                .put("ldap.bind-password", "password1234")
                .put("ldap.allow-user-not-exist", "true")
                .put("ldap.group-base-dn", "ou=Roles,dc=test,dc=com")
                .put("ldap.group-membership-attribute", "member")
                .put("ldap.group-user-membership-attribute", "memberOf")
                .put("ldap.cache-ttl", "2m")
                .buildOrThrow();

        LdapGroupProviderConfig expected = new LdapGroupProviderConfig()
                .setUserBindSearchPatterns(ImmutableList.of("uid=${USER},ou=org,dc=test,dc=com", "uid=${USER},ou=alt"))
                .setUserBaseDistinguishedName("dc=test,dc=com")
                .setBindDistinguishedName("CN=User Name,OU=CITY_OU,OU=STATE_OU,DC=domain,DC=domain_root")
                .setGroupNameAttribute("cn")
                .setBindPassword("password1234")
                .setGroupBaseDistinguishedName("ou=Roles,dc=test,dc=com")
                .setAllowUserNotExist(true)
                .setGroupMembershipAttribute("member")
                .setGroupUserMembershipAttribute("memberOf")
                .setLdapCacheTtl(new Duration(2, TimeUnit.MINUTES));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testExplicitConfigRFC2037()
    {
        assertValidates(new LdapGroupProviderConfig()
                .setUserBindSearchPatterns(ImmutableList.of("cn=${USER},ou=users,dc=example,dc=com"))
                .setUserBaseDistinguishedName("dc=example,dc=com")
                .setBindDistinguishedName("cn=ldap-bind-user,ou=users,dc=example,dc=com")
                .setGroupNameAttribute("cn")
                .setBindPassword("password1234")
                .setGroupBaseDistinguishedName("ou=groups,dc=example,dc=com")
                .setAllowUserNotExist(true)
                .setGroupMembershipAttribute("member")
                .setLdapCacheTtl(new Duration(2, TimeUnit.MINUTES)));
    }

    @Test
    public void testValidationRFC2037bis()
    {
        assertValidates(new LdapGroupProviderConfig()
                .setUserBindSearchPatterns(ImmutableList.of("cn=${USER},ou=users,dc=example,dc=com"))
                .setUserBaseDistinguishedName("dc=example,dc=com")
                .setBindDistinguishedName("cn=ldap-bind-user,ou=users,dc=example,dc=com")
                .setGroupNameAttribute("cn")
                .setBindPassword("password1234")
                .setGroupBaseDistinguishedName("ou=groups,dc=example,dc=com")
                .setAllowUserNotExist(true)
                .setGroupMembershipAttribute("member")
                .setGroupUserMembershipAttribute("memberOf")
                .setLdapCacheTtl(new Duration(2, TimeUnit.MINUTES)));
    }

    @Test
    public void testValidationRFC2037bisNoGroup()
    {
        assertValidates(new LdapGroupProviderConfig()
                .setUserBindSearchPatterns(ImmutableList.of("cn=${USER},ou=users,dc=example,dc=com"))
                .setUserBaseDistinguishedName("dc=example,dc=com")
                .setBindDistinguishedName("cn=ldap-bind-user,ou=users,dc=example,dc=com")
                .setBindPassword("password1234")
                .setAllowUserNotExist(true)
                .setGroupUserMembershipAttribute("memberOf")
                .setLdapCacheTtl(new Duration(2, TimeUnit.MINUTES)));
    }
}
