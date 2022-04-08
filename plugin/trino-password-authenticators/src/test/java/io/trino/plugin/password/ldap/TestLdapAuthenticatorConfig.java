
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

public class TestLdapAuthenticatorConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(LdapAuthenticatorConfig.class)
                .setUserBindSearchPatterns(" : ")
                .setUserBaseDistinguishedName(null)
                .setGroupAuthorizationSearchPattern(null)
                .setBindDistingushedName(null)
                .setBindPassword(null)
                .setLdapCacheTtl(new Duration(1, TimeUnit.HOURS)));
    }

    @Test
    public void testExplicitConfig()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("ldap.user-bind-pattern", "uid=${USER},ou=org,dc=test,dc=com:uid=${USER},ou=alt")
                .put("ldap.user-base-dn", "dc=test,dc=com")
                .put("ldap.group-auth-pattern", "&(objectClass=user)(memberOf=cn=group)(user=username)")
                .put("ldap.bind-dn", "CN=User Name,OU=CITY_OU,OU=STATE_OU,DC=domain,DC=domain_root")
                .put("ldap.bind-password", "password1234")
                .put("ldap.cache-ttl", "2m")
                .buildOrThrow();

        LdapAuthenticatorConfig expected = new LdapAuthenticatorConfig()
                .setUserBindSearchPatterns(ImmutableList.of("uid=${USER},ou=org,dc=test,dc=com", "uid=${USER},ou=alt"))
                .setUserBaseDistinguishedName("dc=test,dc=com")
                .setGroupAuthorizationSearchPattern("&(objectClass=user)(memberOf=cn=group)(user=username)")
                .setBindDistingushedName("CN=User Name,OU=CITY_OU,OU=STATE_OU,DC=domain,DC=domain_root")
                .setBindPassword("password1234")
                .setLdapCacheTtl(new Duration(2, TimeUnit.MINUTES));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertValidates(new LdapAuthenticatorConfig()
                .setUserBindSearchPatterns("uid=${USER},ou=org,dc=test,dc=com")
                .setUserBaseDistinguishedName("dc=test,dc=com")
                .setGroupAuthorizationSearchPattern("&(objectClass=user)(memberOf=cn=group)(user=username)"));

        assertValidates(new LdapAuthenticatorConfig()
                .setUserBindSearchPatterns("uid=${USER},ou=org,dc=test,dc=com")
                .setUserBaseDistinguishedName("dc=test,dc=com")
                .setGroupAuthorizationSearchPattern("&(objectClass=user)(memberOf=cn=group)(user=username)"));
    }
}
