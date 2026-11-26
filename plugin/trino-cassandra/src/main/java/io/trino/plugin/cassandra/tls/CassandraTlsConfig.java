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
package io.trino.plugin.cassandra.tls;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.AssertTrue;

import java.io.File;
import java.util.Optional;

public class CassandraTlsConfig
{
    private File keystorePath;
    private String keystorePassword;
    private File truststorePath;
    private String truststorePassword;
    private boolean verifyHostnames = true;
    private boolean useReverseDns;

    public Optional<@FileExists File> getKeystorePath()
    {
        return Optional.ofNullable(keystorePath);
    }

    @Config("cassandra.tls.keystore-path")
    public CassandraTlsConfig setKeystorePath(File keystorePath)
    {
        this.keystorePath = keystorePath;
        return this;
    }

    public Optional<String> getKeystorePassword()
    {
        return Optional.ofNullable(keystorePassword);
    }

    @Config("cassandra.tls.keystore-password")
    @ConfigSecuritySensitive
    public CassandraTlsConfig setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
        return this;
    }

    public Optional<@FileExists File> getTruststorePath()
    {
        return Optional.ofNullable(truststorePath);
    }

    @Config("cassandra.tls.truststore-path")
    public CassandraTlsConfig setTruststorePath(File truststorePath)
    {
        this.truststorePath = truststorePath;
        return this;
    }

    public Optional<String> getTruststorePassword()
    {
        return Optional.ofNullable(truststorePassword);
    }

    @Config("cassandra.tls.truststore-password")
    @ConfigSecuritySensitive
    public CassandraTlsConfig setTruststorePassword(String truststorePassword)
    {
        this.truststorePassword = truststorePassword;
        return this;
    }

    public boolean isVerifyHostnames()
    {
        return verifyHostnames;
    }

    @Config("cassandra.tls.verify-hostnames")
    @ConfigDescription("Verify that the certificate presented by a Cassandra node matches the host name or IP address of that node")
    public CassandraTlsConfig setVerifyHostnames(boolean verifyHostnames)
    {
        this.verifyHostnames = verifyHostnames;
        return this;
    }

    public boolean isUseReverseDns()
    {
        return useReverseDns;
    }

    @Config("cassandra.tls.use-reverse-dns")
    @ConfigDescription("Verify the certificate of a node addressed by IP against its reverse DNS name instead of the IP address, as the Cassandra driver does by default. Less secure")
    public CassandraTlsConfig setUseReverseDns(boolean useReverseDns)
    {
        this.useReverseDns = useReverseDns;
        return this;
    }

    // The default trust anchors of the JVM contain public certificate authorities, which issue a certificate to the owner of any domain.
    // Such a certificate says nothing unless it is verified against the address used to connect.
    @AssertTrue(message = "cassandra.tls.keystore-path or cassandra.tls.truststore-path must be set when cassandra.tls.verify-hostnames is false or cassandra.tls.use-reverse-dns is true")
    public boolean isKeystoreOrTruststoreConfigured()
    {
        return keystorePath != null || truststorePath != null || (verifyHostnames && !useReverseDns);
    }
}
