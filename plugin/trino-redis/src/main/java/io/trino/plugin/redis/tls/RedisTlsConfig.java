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
package io.trino.plugin.redis.tls;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;

import java.io.File;
import java.util.Optional;

public class RedisTlsConfig
{
    private File keystorePath;
    private String keystorePassword;
    private File truststorePath;
    private String truststorePassword;

    public Optional<@FileExists File> getKeystorePath()
    {
        return Optional.ofNullable(keystorePath);
    }

    @Config("redis.tls.keystore-path")
    @ConfigDescription("Path to the JKS or PKCS12 key store file")
    public RedisTlsConfig setKeystorePath(File keystorePath)
    {
        this.keystorePath = keystorePath;
        return this;
    }

    public Optional<String> getKeystorePassword()
    {
        return Optional.ofNullable(keystorePassword);
    }

    @Config("redis.tls.keystore-password")
    @ConfigSecuritySensitive
    @ConfigDescription("Password for the key store")
    public RedisTlsConfig setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
        return this;
    }

    public Optional<@FileExists File> getTruststorePath()
    {
        return Optional.ofNullable(truststorePath);
    }

    @Config("redis.tls.truststore-path")
    @ConfigDescription("Path to the JKS or PKCS12 trust store file")
    public RedisTlsConfig setTruststorePath(File truststorePath)
    {
        this.truststorePath = truststorePath;
        return this;
    }

    public Optional<String> getTruststorePassword()
    {
        return Optional.ofNullable(truststorePassword);
    }

    @Config("redis.tls.truststore-password")
    @ConfigSecuritySensitive
    @ConfigDescription("Password for the trust store")
    public RedisTlsConfig setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
        return this;
    }
}
