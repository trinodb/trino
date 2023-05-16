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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.Duration;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class LdapGroupProviderConfig
{
    private List<String> userBindSearchPatterns = ImmutableList.of();
    private String userBaseDistinguishedName;
    private String groupBaseDistinguishedName;
    private String groupNameAttribute;
    private String bindDistinguishedName;
    private String bindPassword;
    private boolean allowUserNotExist;
    private String groupMembershipAttribute;
    private String groupUserMembershipAttribute;
    private Duration ldapCacheTtl = new Duration(1, TimeUnit.HOURS);

    @Nullable
    public String getGroupBaseDistinguishedName()
    {
        return groupBaseDistinguishedName;
    }

    @Config("ldap.group-base-dn")
    @ConfigDescription("Base distinguished name of the group. Example: ou=groups,dc=example,dc=com")
    public LdapGroupProviderConfig setGroupBaseDistinguishedName(String groupBaseDistinguishedName)
    {
        this.groupBaseDistinguishedName = groupBaseDistinguishedName;
        return this;
    }

    @Nullable
    public String getGroupNameAttribute()
    {
        return groupNameAttribute;
    }

    @Config("ldap.group-name-attribute")
    @ConfigDescription("Value used to identity the group name Example: cn")
    public LdapGroupProviderConfig setGroupNameAttribute(String groupNameAttribute)
    {
        this.groupNameAttribute = groupNameAttribute;
        return this;
    }

    @NotNull
    @NotEmpty
    public List<String> getUserBindSearchPatterns()
    {
        return userBindSearchPatterns;
    }

    public LdapGroupProviderConfig setUserBindSearchPatterns(List<String> userBindSearchPatterns)
    {
        this.userBindSearchPatterns = requireNonNull(userBindSearchPatterns, "userBindSearchPatterns is null");
        return this;
    }

    @Config("ldap.user-bind-pattern")
    @ConfigDescription("User bind pattern. Examples: ${USER}@example.com, uid=${USER}")
    public LdapGroupProviderConfig setUserBindSearchPatterns(String userBindSearchPatterns)
    {
        this.userBindSearchPatterns = Splitter.on(":")
                .trimResults()
                .omitEmptyStrings()
                .splitToList(userBindSearchPatterns);
        return this;
    }

    @NotNull
    @NotEmpty
    public String getUserBaseDistinguishedName()
    {
        return userBaseDistinguishedName;
    }

    @Config("ldap.user-base-dn")
    @ConfigDescription("Base distinguished name of the user. Example: dc=example,dc=com")
    public LdapGroupProviderConfig setUserBaseDistinguishedName(String userBaseDistinguishedName)
    {
        this.userBaseDistinguishedName = userBaseDistinguishedName;
        return this;
    }

    @NotNull
    @NotEmpty
    public String getBindDistinguishedName()
    {
        return bindDistinguishedName;
    }

    @Config("ldap.bind-dn")
    @ConfigDescription("Bind distinguished name. Example: CN=User Name,OU=CITY_OU,OU=STATE_OU,DC=domain,DC=domain_root")
    public LdapGroupProviderConfig setBindDistinguishedName(String bindDistinguishedName)
    {
        this.bindDistinguishedName = bindDistinguishedName;
        return this;
    }

    @NotNull
    @NotEmpty
    public String getBindPassword()
    {
        return bindPassword;
    }

    @Config("ldap.bind-password")
    @ConfigDescription("Bind password used. Example: password1234")
    @ConfigSecuritySensitive
    public LdapGroupProviderConfig setBindPassword(String bindPassword)
    {
        this.bindPassword = bindPassword;
        return this;
    }

    public boolean isAllowUserNotExist()
    {
        return allowUserNotExist;
    }

    @Config("ldap.allow-user-not-exist")
    @ConfigDescription("Don't throw an exception if user does not exist")
    public LdapGroupProviderConfig setAllowUserNotExist(boolean allowUserNotExist)
    {
        this.allowUserNotExist = allowUserNotExist;
        return this;
    }

    @NotNull
    public Duration getLdapCacheTtl()
    {
        return ldapCacheTtl;
    }

    @Config("ldap.cache-ttl")
    public LdapGroupProviderConfig setLdapCacheTtl(Duration ldapCacheTtl)
    {
        this.ldapCacheTtl = ldapCacheTtl;
        return this;
    }

    @Nullable
    public String getGroupMembershipAttribute()
    {
        return groupMembershipAttribute;
    }

    @Config("ldap.group-membership-attribute")
    public LdapGroupProviderConfig setGroupMembershipAttribute(String groupMembershipAttribute)
    {
        this.groupMembershipAttribute = groupMembershipAttribute;
        return this;
    }

    @Nullable
    public String getGroupUserMembershipAttribute()
    {
        return groupUserMembershipAttribute;
    }

    @Config("ldap.group-user-membership-attribute")
    public LdapGroupProviderConfig setGroupUserMembershipAttribute(String groupUserMembershipAttribute)
    {
        this.groupUserMembershipAttribute = groupUserMembershipAttribute;
        return this;
    }
}
