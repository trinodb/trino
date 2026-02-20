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
package io.trino.tests.product.ldap;

/**
 * LDAP environment that searches from the referral-enabled world base DN.
 */
public class LdapReferralsEnvironment
        extends LdapBasicEnvironment
{
    @Override
    protected String getOpenLdapImage()
    {
        return OPENLDAP_REFERRALS_IMAGE;
    }

    @Override
    protected String getLdapPasswordAuthenticatorProperties()
    {
        return """
                password-authenticator.name=ldap
                ldap.url=ldaps://ldapserver:636
                ldap.ssl.keystore.path=%s/%s
                ldap.ssl.truststore.path=%s/openldap-certificate.pem
                ldap.user-bind-pattern=uid=${USER},ou=America,dc=trino,dc=testldap,dc=com:uid=${USER},ou=Asia,dc=trino,dc=testldap,dc=com
                ldap.user-base-dn=ou=World,dc=trino,dc=testldap,dc=com
                ldap.ignore-referrals=false
                ldap.group-auth-pattern=(&(objectClass=inetOrgPerson)(uid=${USER})(memberof=cn=DefaultGroup,ou=America,dc=trino,dc=testldap,dc=com))
                """.formatted("/etc/trino/certs", TRINO_LDAP_CLIENT_CERT_FILE, "/etc/trino/certs");
    }
}
