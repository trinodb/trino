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
package io.trino.plugin.base.ldap;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.LegacyConfig;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

import java.io.File;
import java.util.Optional;

import static com.google.common.base.Strings.nullToEmpty;

public class LdapClientConfig
{
    private String ldapUrl;
    private boolean allowInsecure;
    private File keystorePath;
    private String keystorePassword;
    private File trustStorePath;
    private String truststorePassword;
    private boolean ignoreReferrals;
    private Optional<Duration> ldapConnectionTimeout = Optional.empty();
    private Optional<Duration> ldapReadTimeout = Optional.empty();

    @NotNull
    @Pattern(regexp = "^ldaps?://.*", message = "Invalid LDAP server URL. Expected ldap:// or ldaps://")
    public String getLdapUrl()
    {
        return ldapUrl;
    }

    @Config("ldap.url")
    @ConfigDescription("URL of the LDAP server")
    public LdapClientConfig setLdapUrl(String url)
    {
        this.ldapUrl = url;
        return this;
    }

    public boolean isAllowInsecure()
    {
        return allowInsecure;
    }

    @Config("ldap.allow-insecure")
    @ConfigDescription("Allow insecure connection to the LDAP server")
    public LdapClientConfig setAllowInsecure(boolean allowInsecure)
    {
        this.allowInsecure = allowInsecure;
        return this;
    }

    @AssertTrue(message = "Connecting to the LDAP server without SSL enabled requires `ldap.allow-insecure=true`")
    public boolean isUrlConfigurationValid()
    {
        return nullToEmpty(ldapUrl).startsWith("ldaps://") || allowInsecure;
    }

    public Optional<@FileExists File> getKeystorePath()
    {
        return Optional.ofNullable(keystorePath);
    }

    @Config("ldap.ssl.keystore.path")
    @ConfigDescription("Path to the PEM or JKS key store")
    public LdapClientConfig setKeystorePath(File path)
    {
        this.keystorePath = path;
        return this;
    }

    public Optional<String> getKeystorePassword()
    {
        return Optional.ofNullable(keystorePassword);
    }

    @Config("ldap.ssl.keystore.password")
    @ConfigSecuritySensitive
    @ConfigDescription("Password for the key store")
    public LdapClientConfig setKeystorePassword(String password)
    {
        this.keystorePassword = password;
        return this;
    }

    public Optional<@FileExists File> getTrustStorePath()
    {
        return Optional.ofNullable(trustStorePath);
    }

    @LegacyConfig("ldap.ssl-trust-certificate")
    @Config("ldap.ssl.truststore.path")
    @ConfigDescription("Path to the PEM or JKS trust store")
    public LdapClientConfig setTrustStorePath(File path)
    {
        this.trustStorePath = path;
        return this;
    }

    public Optional<String> getTruststorePassword()
    {
        return Optional.ofNullable(truststorePassword);
    }

    @Config("ldap.ssl.truststore.password")
    @ConfigSecuritySensitive
    @ConfigDescription("Password for the trust store")
    public LdapClientConfig setTruststorePassword(String password)
    {
        this.truststorePassword = password;
        return this;
    }

    public boolean isIgnoreReferrals()
    {
        return ignoreReferrals;
    }

    @Config("ldap.ignore-referrals")
    @ConfigDescription("Referrals allow finding entries across multiple LDAP servers. Ignore them to only search within 1 LDAP server")
    public LdapClientConfig setIgnoreReferrals(boolean ignoreReferrals)
    {
        this.ignoreReferrals = ignoreReferrals;
        return this;
    }

    public Optional<Duration> getLdapConnectionTimeout()
    {
        return ldapConnectionTimeout;
    }

    @Config("ldap.timeout.connect")
    @ConfigDescription("Timeout for establishing a connection")
    public LdapClientConfig setLdapConnectionTimeout(Duration ldapConnectionTimeout)
    {
        this.ldapConnectionTimeout = Optional.ofNullable(ldapConnectionTimeout);
        return this;
    }

    public Optional<Duration> getLdapReadTimeout()
    {
        return ldapReadTimeout;
    }

    @Config("ldap.timeout.read")
    @ConfigDescription("Timeout for reading data from LDAP")
    public LdapClientConfig setLdapReadTimeout(Duration ldapReadTimeout)
    {
        this.ldapReadTimeout = Optional.ofNullable(ldapReadTimeout);
        return this;
    }
}
