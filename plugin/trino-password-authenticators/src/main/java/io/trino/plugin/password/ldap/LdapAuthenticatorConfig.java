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

import javax.validation.constraints.NotNull;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class LdapAuthenticatorConfig
{
    private List<String> userBindSearchPatterns = ImmutableList.of();
    private String groupAuthorizationSearchPattern;
    private String userBaseDistinguishedName;
    private String bindDistinguishedName;
    private String bindPassword;
    private Duration ldapCacheTtl = new Duration(1, TimeUnit.HOURS);

    @NotNull
    public List<String> getUserBindSearchPatterns()
    {
        return userBindSearchPatterns;
    }

    public LdapAuthenticatorConfig setUserBindSearchPatterns(List<String> userBindSearchPatterns)
    {
        this.userBindSearchPatterns = requireNonNull(userBindSearchPatterns, "userBindSearchPatterns is null");
        return this;
    }

    @Config("ldap.user-bind-pattern")
    @ConfigDescription("Custom user bind pattern. Example: ${USER}@example.com")
    public LdapAuthenticatorConfig setUserBindSearchPatterns(String userBindSearchPatterns)
    {
        this.userBindSearchPatterns = Splitter.on(":")
                .trimResults()
                .omitEmptyStrings()
                .splitToList(userBindSearchPatterns);
        return this;
    }

    public String getGroupAuthorizationSearchPattern()
    {
        return groupAuthorizationSearchPattern;
    }

    @Config("ldap.group-auth-pattern")
    @ConfigDescription("Custom group authorization check query. Example: &(objectClass=user)(memberOf=cn=group)(user=username)")
    public LdapAuthenticatorConfig setGroupAuthorizationSearchPattern(String groupAuthorizationSearchPattern)
    {
        this.groupAuthorizationSearchPattern = groupAuthorizationSearchPattern;
        return this;
    }

    public String getUserBaseDistinguishedName()
    {
        return userBaseDistinguishedName;
    }

    @Config("ldap.user-base-dn")
    @ConfigDescription("Base distinguished name of the user. Example: dc=example,dc=com")
    public LdapAuthenticatorConfig setUserBaseDistinguishedName(String userBaseDistinguishedName)
    {
        this.userBaseDistinguishedName = userBaseDistinguishedName;
        return this;
    }

    public String getBindDistingushedName()
    {
        return bindDistinguishedName;
    }

    @Config("ldap.bind-dn")
    @ConfigDescription("Bind distinguished name. Example: CN=User Name,OU=CITY_OU,OU=STATE_OU,DC=domain,DC=domain_root")
    public LdapAuthenticatorConfig setBindDistingushedName(String bindDistingushedName)
    {
        this.bindDistinguishedName = bindDistingushedName;
        return this;
    }

    public String getBindPassword()
    {
        return bindPassword;
    }

    @Config("ldap.bind-password")
    @ConfigDescription("Bind password used. Example: password1234")
    @ConfigSecuritySensitive
    public LdapAuthenticatorConfig setBindPassword(String bindPassword)
    {
        this.bindPassword = bindPassword;
        return this;
    }

    @NotNull
    public Duration getLdapCacheTtl()
    {
        return ldapCacheTtl;
    }

    @Config("ldap.cache-ttl")
    public LdapAuthenticatorConfig setLdapCacheTtl(Duration ldapCacheTtl)
    {
        this.ldapCacheTtl = ldapCacheTtl;
        return this;
    }
}
