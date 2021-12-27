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
package io.trino.server;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.validation.FileExists;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;

import java.util.Optional;

@DefunctConfig({
        "internal-communication.kerberos.enabled",
        "internal-communication.kerberos.use-canonical-hostname",
        "internal-communication.jwt.enabled",
})
public class InternalCommunicationConfig
{
    private String sharedSecret;
    private boolean http2Enabled;
    private boolean httpsRequired;
    private String keyStorePath;
    private String keyStorePassword;
    private String trustStorePath;
    private String trustStorePassword;
    private boolean httpServerHttpsEnabled;

    @NotNull
    public Optional<String> getSharedSecret()
    {
        return Optional.ofNullable(sharedSecret);
    }

    @ConfigSecuritySensitive
    @Config("internal-communication.shared-secret")
    public InternalCommunicationConfig setSharedSecret(String sharedSecret)
    {
        this.sharedSecret = sharedSecret;
        return this;
    }

    public boolean isHttp2Enabled()
    {
        return http2Enabled;
    }

    @Config("internal-communication.http2.enabled")
    @ConfigDescription("Enable the HTTP/2 transport")
    public InternalCommunicationConfig setHttp2Enabled(boolean http2Enabled)
    {
        this.http2Enabled = http2Enabled;
        return this;
    }

    public boolean isHttpsRequired()
    {
        return httpsRequired;
    }

    @Config("internal-communication.https.required")
    public InternalCommunicationConfig setHttpsRequired(boolean httpsRequired)
    {
        this.httpsRequired = httpsRequired;
        return this;
    }

    @FileExists
    public String getKeyStorePath()
    {
        return keyStorePath;
    }

    @Config("internal-communication.https.keystore.path")
    public InternalCommunicationConfig setKeyStorePath(String keyStorePath)
    {
        this.keyStorePath = keyStorePath;
        return this;
    }

    public String getKeyStorePassword()
    {
        return keyStorePassword;
    }

    @Config("internal-communication.https.keystore.key")
    @ConfigSecuritySensitive
    public InternalCommunicationConfig setKeyStorePassword(String keyStorePassword)
    {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    @FileExists
    public String getTrustStorePath()
    {
        return trustStorePath;
    }

    @Config("internal-communication.https.truststore.path")
    public InternalCommunicationConfig setTrustStorePath(String trustStorePath)
    {
        this.trustStorePath = trustStorePath;
        return this;
    }

    public String getTrustStorePassword()
    {
        return trustStorePassword;
    }

    @Config("internal-communication.https.truststore.key")
    @ConfigSecuritySensitive
    public InternalCommunicationConfig setTrustStorePassword(String trustStorePassword)
    {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    @AssertTrue(message = "Internal shared secret is required when HTTPS is enabled for internal communications")
    public boolean isRequiredSharedSecretSet()
    {
        return !isHttpsRequired() || getSharedSecret().isPresent();
    }

    public boolean isHttpServerHttpsEnabled()
    {
        return httpServerHttpsEnabled;
    }

    @Config("http-server.https.enabled")
    public InternalCommunicationConfig setHttpServerHttpsEnabled(boolean httpServerHttpsEnabled)
    {
        this.httpServerHttpsEnabled = httpServerHttpsEnabled;
        return this;
    }

    @AssertTrue(message = "HTTPS must be enabled when HTTPS is required for internal communications. Set http-server.https.enabled=true")
    public boolean isHttpsEnabledWhenRequired()
    {
        return !isHttpsRequired() || isHttpServerHttpsEnabled();
    }
}
