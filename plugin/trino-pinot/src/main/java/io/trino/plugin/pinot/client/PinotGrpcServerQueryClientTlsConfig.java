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

import io.airlift.configuration.Config;

import static io.trino.plugin.pinot.client.PinotKeystoreTrustStoreType.JKS;

public class PinotGrpcServerQueryClientTlsConfig
{
    private PinotKeystoreTrustStoreType keystoreType = JKS;
    private String keystorePath;
    private String keystorePassword;
    private PinotKeystoreTrustStoreType truststoreType = JKS;
    private String truststorePath;
    private String truststorePassword;
    private String sslProvider = "JDK";

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

    public String getKeystorePath()
    {
        return keystorePath;
    }

    @Config("pinot.grpc.tls.keystore-path")
    public PinotGrpcServerQueryClientTlsConfig setKeystorePath(String keystorePath)
    {
        this.keystorePath = keystorePath;
        return this;
    }

    public String getKeystorePassword()
    {
        return keystorePassword;
    }

    @Config("pinot.grpc.tls.keystore-password")
    public PinotGrpcServerQueryClientTlsConfig setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
        return this;
    }

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

    public String getTruststorePath()
    {
        return truststorePath;
    }

    @Config("pinot.grpc.tls.truststore-path")
    public PinotGrpcServerQueryClientTlsConfig setTruststorePath(String truststorePath)
    {
        this.truststorePath = truststorePath;
        return this;
    }

    public String getTruststorePassword()
    {
        return truststorePassword;
    }

    @Config("pinot.grpc.tls.truststore-password")
    public PinotGrpcServerQueryClientTlsConfig setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
        return this;
    }

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
}
