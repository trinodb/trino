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
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

public class LdapGroupProviderConfig
{
    private String ldapAdminUser;
    private String ldapAdminPassword;
    private String ldapUserBaseDN;
    private String ldapUserSearchFilter = "(uid={0})";
    private String ldapGroupsNameAttribute = "cn";
    private boolean ldapUseGroupFilter;

    @NotNull
    public String getLdapAdminUser()
    {
        return ldapAdminUser;
    }

    @Config("ldap.admin-user")
    @ConfigDescription("Bind distinguished name for admin user. Example: CN=User Name,OU=CITY_OU,OU=STATE_OU,DC=domain,DC=domain_root")
    public LdapGroupProviderConfig setLdapAdminUser(String ldapAdminUser)
    {
        this.ldapAdminUser = ldapAdminUser;
        return this;
    }

    @NotNull
    public String getLdapAdminPassword()
    {
        return ldapAdminPassword;
    }

    @ConfigSecuritySensitive
    @Config("ldap.admin-password")
    @ConfigDescription("Bind password used for admin user. Example: password1234")
    public LdapGroupProviderConfig setLdapAdminPassword(String ldapAdminPassword)
    {
        this.ldapAdminPassword = ldapAdminPassword;
        return this;
    }

    @NotNull
    public String getLdapUserBaseDN()
    {
        return ldapUserBaseDN;
    }

    @Config("ldap.user-base-dn")
    @ConfigDescription("Base distinguished name for users. Example: dc=example,dc=com")
    public LdapGroupProviderConfig setLdapUserBaseDN(String ldapUserBaseDN)
    {
        this.ldapUserBaseDN = ldapUserBaseDN;
        return this;
    }

    @NotNull
    public String getLdapUserSearchFilter()
    {
        return ldapUserSearchFilter;
    }

    @Config("ldap.user-search-filter")
    @ConfigDescription("Search filter for user documents, templated with the Trino username. Example: (cn={0})")
    public LdapGroupProviderConfig setLdapUserSearchFilter(String ldapUserSearchFilter)
    {
        this.ldapUserSearchFilter = ldapUserSearchFilter;
        return this;
    }

    @NotNull
    public String getLdapGroupsNameAttribute()
    {
        return ldapGroupsNameAttribute;
    }

    @Config("ldap.group-name-attribute")
    @ConfigDescription("Attribute from group documents used as the name. Example: cn")
    public LdapGroupProviderConfig setLdapGroupsNameAttribute(String ldapGroupsNameAttribute)
    {
        this.ldapGroupsNameAttribute = ldapGroupsNameAttribute;
        return this;
    }

    public boolean getLdapUseGroupFilter()
    {
        return ldapUseGroupFilter;
    }

    @Config("ldap.use-group-filter")
    @ConfigDescription("Enable group filtering")
    public LdapGroupProviderConfig setLdapUseGroupFilter(boolean ldapUseGroupFilter)
    {
        this.ldapUseGroupFilter = ldapUseGroupFilter;
        return this;
    }
}
