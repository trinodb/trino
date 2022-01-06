
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

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;

public class TestLdapConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(LdapConfig.class)
                .setLdapUrl(null)
                .setAllowInsecure(false)
                .setTrustCertificate(null)
                .setIgnoreCertificate(false)
                .setUserBindSearchPatterns(" : ")
                .setUserBaseDistinguishedName(null)
                .setGroupAuthorizationSearchPattern(null)
                .setBindDistingushedName(null)
                .setBindPassword(null)
                .setIgnoreReferrals(false)
                .setLdapCacheTtl(new Duration(1, TimeUnit.HOURS)));
    }

    @Test
    public void testExplicitConfig()
            throws IOException
    {
        Path trustCertificateFile = Files.createTempFile(null, null);

        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("ldap.url", "ldaps://localhost:636")
                .put("ldap.allow-insecure", "true")
                .put("ldap.ssl-trust-certificate", trustCertificateFile.toString())
                .put("ldap.ignore-ssl-certificate", "true")
                .put("ldap.user-bind-pattern", "uid=${USER},ou=org,dc=test,dc=com:uid=${USER},ou=alt")
                .put("ldap.user-base-dn", "dc=test,dc=com")
                .put("ldap.group-auth-pattern", "&(objectClass=user)(memberOf=cn=group)(user=username)")
                .put("ldap.bind-dn", "CN=User Name,OU=CITY_OU,OU=STATE_OU,DC=domain,DC=domain_root")
                .put("ldap.bind-password", "password1234")
                .put("ldap.ignore-referrals", "true")
                .put("ldap.cache-ttl", "2m")
                .build();

        LdapConfig expected = new LdapConfig()
                .setLdapUrl("ldaps://localhost:636")
                .setAllowInsecure(true)
                .setTrustCertificate(trustCertificateFile.toFile())
                .setIgnoreCertificate(true)
                .setUserBindSearchPatterns(ImmutableList.of("uid=${USER},ou=org,dc=test,dc=com", "uid=${USER},ou=alt"))
                .setUserBaseDistinguishedName("dc=test,dc=com")
                .setGroupAuthorizationSearchPattern("&(objectClass=user)(memberOf=cn=group)(user=username)")
                .setBindDistingushedName("CN=User Name,OU=CITY_OU,OU=STATE_OU,DC=domain,DC=domain_root")
                .setBindPassword("password1234")
                .setIgnoreReferrals(true)
                .setLdapCacheTtl(new Duration(2, TimeUnit.MINUTES));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertValidates(new LdapConfig()
                .setLdapUrl("ldaps://localhost")
                .setUserBindSearchPatterns("uid=${USER},ou=org,dc=test,dc=com")
                .setUserBaseDistinguishedName("dc=test,dc=com")
                .setGroupAuthorizationSearchPattern("&(objectClass=user)(memberOf=cn=group)(user=username)"));

        assertValidates(new LdapConfig()
                .setLdapUrl("ldap://localhost")
                .setAllowInsecure(true)
                .setUserBindSearchPatterns("uid=${USER},ou=org,dc=test,dc=com")
                .setUserBaseDistinguishedName("dc=test,dc=com")
                .setGroupAuthorizationSearchPattern("&(objectClass=user)(memberOf=cn=group)(user=username)"));

        assertFailsValidation(
                new LdapConfig()
                        .setLdapUrl("ldap://")
                        .setAllowInsecure(false),
                "urlConfigurationValid",
                "Connecting to the LDAP server without SSL enabled requires `ldap.allow-insecure=true`",
                AssertTrue.class);

        assertFailsValidation(new LdapConfig().setLdapUrl("localhost"), "ldapUrl", "Invalid LDAP server URL. Expected ldap:// or ldaps://", Pattern.class);
        assertFailsValidation(new LdapConfig().setLdapUrl("ldaps:/localhost"), "ldapUrl", "Invalid LDAP server URL. Expected ldap:// or ldaps://", Pattern.class);

        assertFailsValidation(new LdapConfig(), "ldapUrl", "may not be null", NotNull.class);
    }
}
