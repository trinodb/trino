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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

import java.util.Set;

import static com.google.common.base.Strings.nullToEmpty;

public class LdapGroupProviderConfig
{
    private String ldapAdminUser;
    private String ldapAdminPassword;
    private String ldapUserBaseDN;
    private String ldapUserSearchFilter = "(uid={0})";
    private Set<String> ldapUserSearchAttributes = Set.of();
    private String ldapUserMemberOfAttribute;
    private String ldapGroupsNameAttribute = "cn";

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
    public Set<String> getLdapUserSearchAttributes()
    {
        return ldapUserSearchAttributes;
    }

    @Config("ldap.user-search-attributes")
    @ConfigDescription("User attributes loaded as groups. Example: cn, uuid")
    public LdapGroupProviderConfig setLdapUserSearchAttributes(String ldapUserSearchAttributes)
    {
        this.ldapUserSearchAttributes = ImmutableSet.copyOf(Splitter.on(",").omitEmptyStrings().trimResults().split(nullToEmpty(ldapUserSearchAttributes)));
        return this;
    }

    public String getLdapUserMemberOfAttribute()
    {
        return ldapUserMemberOfAttribute;
    }

    @Config("ldap.user-member-of-attribute")
    @ConfigDescription("Group membership attribute in user documents. Example: memberOf")
    public LdapGroupProviderConfig setLdapUserMemberOfAttribute(String ldapUserMemberOfAttribute)
    {
        this.ldapUserMemberOfAttribute = ldapUserMemberOfAttribute;
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
}
