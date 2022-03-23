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

import javax.naming.NamingException;

import java.util.Set;

public interface LdapAuthenticatorClient
{
    void validatePassword(String userDistinguishedName, String password)
            throws NamingException;

    boolean isGroupMember(String searchBase, String groupSearch, String contextUserDistinguishedName, String contextPassword)
            throws NamingException;

    Set<String> lookupUserDistinguishedNames(String searchBase, String searchFilter, String contextUserDistinguishedName, String contextPassword)
            throws NamingException;

    Set<String> lookupUserAttributeValues(String searchBase, String searchFilter, String contextUserDistinguishedName, String contextPassword, String attributeName)
            throws NamingException;
}
