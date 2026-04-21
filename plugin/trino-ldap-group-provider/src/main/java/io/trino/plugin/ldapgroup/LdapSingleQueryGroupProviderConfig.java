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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotEmpty;

public class LdapSingleQueryGroupProviderConfig
{
    private String ldapUserMemberOfAttribute = "memberOf";

    @NotEmpty
    public String getLdapUserMemberOfAttribute()
    {
        return ldapUserMemberOfAttribute;
    }

    @Config("ldap.user-member-of-attribute")
    @ConfigDescription("Group membership attribute in user documents. Example: memberOf")
    public LdapSingleQueryGroupProviderConfig setLdapUserMemberOfAttribute(String ldapUserMemberOfAttribute)
    {
        this.ldapUserMemberOfAttribute = ldapUserMemberOfAttribute;
        return this;
    }
}
