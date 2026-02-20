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
 * LDAP environment that uses insecure LDAP over port 389 instead of LDAPS.
 */
public class LdapInsecureEnvironment
        extends LdapBasicEnvironment
{
    @Override
    protected String getLdapPasswordAuthenticatorProperties()
    {
        return """
                password-authenticator.name=ldap
                ldap.url=ldap://ldapserver:389
                ldap.allow-insecure=true
                ldap.user-bind-pattern=uid=${USER},ou=America,dc=trino,dc=testldap,dc=com:uid=${USER},ou=Asia,dc=trino,dc=testldap,dc=com
                ldap.user-base-dn=dc=trino,dc=testldap,dc=com
                ldap.group-auth-pattern=(&(objectClass=inetOrgPerson)(uid=${USER})(memberof=cn=DefaultGroup,ou=America,dc=trino,dc=testldap,dc=com))
                """;
    }
}
