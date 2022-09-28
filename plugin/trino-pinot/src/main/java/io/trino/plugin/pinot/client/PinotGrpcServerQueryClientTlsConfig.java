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
package io.trino.plugin.pinot.client;

import com.google.common.collect.ImmutableList;
import com.google.inject.ConfigurationException;
import com.google.inject.spi.Message;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;

import javax.annotation.PostConstruct;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.Optional;

import static io.trino.plugin.pinot.client.PinotKeystoreTrustStoreType.JKS;

public class PinotGrpcServerQueryClientTlsConfig
{
    private PinotKeystoreTrustStoreType keystoreType = JKS;
    private File keystorePath;
    private String keystorePassword;
    private PinotKeystoreTrustStoreType truststoreType = JKS;
    private File truststorePath;
    private String truststorePassword;
    private String sslProvider = "JDK";

    @NotNull
    public PinotKeystoreTrustStoreType getKeystoreType()
    {
        return keystoreType;
    }

    @Config("pinot.grpc.tls.keystore-type")
    public PinotGrpcServerQueryClientTlsConfig setKeystoreType(PinotKeystoreTrustStoreType keystoreType)
    {
        this.keystoreType = keystoreType;
        return this;
    }

    public Optional<@FileExists File> getKeystorePath()
    {
        return Optional.ofNullable(keystorePath);
    }

    @Config("pinot.grpc.tls.keystore-path")
    public PinotGrpcServerQueryClientTlsConfig setKeystorePath(File keystorePath)
    {
        this.keystorePath = keystorePath;
        return this;
    }

    public Optional<String> getKeystorePassword()
    {
        return Optional.ofNullable(keystorePassword);
    }

    @Config("pinot.grpc.tls.keystore-password")
    @ConfigSecuritySensitive
    public PinotGrpcServerQueryClientTlsConfig setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
        return this;
    }

    @NotNull
    public PinotKeystoreTrustStoreType getTruststoreType()
    {
        return truststoreType;
    }

    @Config("pinot.grpc.tls.truststore-type")
    public PinotGrpcServerQueryClientTlsConfig setTruststoreType(PinotKeystoreTrustStoreType truststoreType)
    {
        this.truststoreType = truststoreType;
        return this;
    }

    public Optional<@FileExists File> getTruststorePath()
    {
        return Optional.ofNullable(truststorePath);
    }

    @Config("pinot.grpc.tls.truststore-path")
    public PinotGrpcServerQueryClientTlsConfig setTruststorePath(File truststorePath)
    {
        this.truststorePath = truststorePath;
        return this;
    }

    public Optional<String> getTruststorePassword()
    {
        return Optional.ofNullable(truststorePassword);
    }

    @Config("pinot.grpc.tls.truststore-password")
    @ConfigSecuritySensitive
    public PinotGrpcServerQueryClientTlsConfig setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
        return this;
    }

    @NotNull
    public String getSslProvider()
    {
        return sslProvider;
    }

    @Config("pinot.grpc.tls.ssl-provider")
    public PinotGrpcServerQueryClientTlsConfig setSslProvider(String sslProvider)
    {
        this.sslProvider = sslProvider;
        return this;
    }

    @PostConstruct
    public void validate()
    {
        if (getKeystorePath().isPresent() && getKeystorePassword().isEmpty()) {
            throw new ConfigurationException(ImmutableList.of(new Message("pinot.grpc.tls.keystore-password must set when pinot.grpc.tls.keystore-path is given")));
        }
        if (getTruststorePath().isPresent() && getTruststorePassword().isEmpty()) {
            throw new ConfigurationException(ImmutableList.of(new Message("pinot.grpc.tls.truststore-password must set when pinot.grpc.tls.truststore-path is given")));
        }
    }
}
