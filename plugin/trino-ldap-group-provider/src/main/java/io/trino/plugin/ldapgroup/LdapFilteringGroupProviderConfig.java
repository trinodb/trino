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
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

public class LdapFilteringGroupProviderConfig
{
    public enum LdapGroupSearchMode
    {
        DIRECT,
        RECURSIVE,
        MATCHING_RULE_IN_CHAIN,
    }

    private String ldapGroupBaseDN;
    private String ldapGroupsSearchFilter;
    private String ldapGroupsSearchMemberAttribute = "member";
    private LdapGroupSearchMode ldapGroupSearchMode = LdapGroupSearchMode.DIRECT;

    @NotNull
    public String getLdapGroupBaseDN()
    {
        return ldapGroupBaseDN;
    }

    @Config("ldap.group-base-dn")
    @ConfigDescription("Base distinguished name for groups. Example: dc=example,dc=com")
    public LdapFilteringGroupProviderConfig setLdapGroupBaseDN(String ldapGroupBaseDN)
    {
        this.ldapGroupBaseDN = ldapGroupBaseDN;
        return this;
    }

    @NotNull
    public Optional<String> getLdapGroupsSearchFilter()
    {
        return Optional.ofNullable(ldapGroupsSearchFilter);
    }

    @Config("ldap.group-search-filter")
    @ConfigDescription("Search filter for group documents. Example: (cn=trino_*)")
    public LdapFilteringGroupProviderConfig setLdapGroupsSearchFilter(String ldapGroupsSearchFilter)
    {
        this.ldapGroupsSearchFilter = ldapGroupsSearchFilter;
        return this;
    }

    @NotNull
    public String getLdapGroupsSearchMemberAttribute()
    {
        return ldapGroupsSearchMemberAttribute;
    }

    @Config("ldap.group-search-member-attribute")
    @ConfigDescription("Attribute from group documents used for filtering by member. Example: cn")
    public LdapFilteringGroupProviderConfig setLdapGroupsSearchMemberAttribute(String ldapGroupsSearchMemberAttribute)
    {
        this.ldapGroupsSearchMemberAttribute = ldapGroupsSearchMemberAttribute;
        return this;
    }

    @NotNull
    public LdapGroupSearchMode getLdapGroupSearchMode()
    {
        return ldapGroupSearchMode;
    }

    @Config("ldap.group-search-mode")
    @ConfigDescription("LDAP group resolution mode")
    public LdapFilteringGroupProviderConfig setLdapGroupSearchMode(LdapGroupSearchMode ldapGroupSearchMode)
    {
        this.ldapGroupSearchMode = ldapGroupSearchMode;
        return this;
    }
}
