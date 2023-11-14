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
package io.trino.plugin.hive.metastore.thrift;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;

import java.io.File;

public class ThriftMetastoreSslConfig
{
    private File keystorePath;
    private String keystorePassword;
    private File truststorePath;
    private String trustStorePassword;

    @FileExists
    public File getKeystorePath()
    {
        return keystorePath;
    }

    @Config("hive.metastore.thrift.client.ssl.key")
    @ConfigDescription("Path to the key store")
    public ThriftMetastoreSslConfig setKeystorePath(File keystorePath)
    {
        this.keystorePath = keystorePath;
        return this;
    }

    public String getKeystorePassword()
    {
        return keystorePassword;
    }

    @Config("hive.metastore.thrift.client.ssl.key-password")
    @ConfigDescription("Password for the key store")
    @ConfigSecuritySensitive
    public ThriftMetastoreSslConfig setKeystorePassword(String keystorePassword)
    {
        this.keystorePassword = keystorePassword;
        return this;
    }

    @FileExists
    public File getTruststorePath()
    {
        return truststorePath;
    }

    @Config("hive.metastore.thrift.client.ssl.trust-certificate")
    @ConfigDescription("Path to the trust store")
    public ThriftMetastoreSslConfig setTruststorePath(File truststorePath)
    {
        this.truststorePath = truststorePath;
        return this;
    }

    public String getTruststorePassword()
    {
        return trustStorePassword;
    }

    @Config("hive.metastore.thrift.client.ssl.trust-certificate-password")
    @ConfigDescription("Password for the trust store")
    @ConfigSecuritySensitive
    public ThriftMetastoreSslConfig setTruststorePassword(String trustStorePassword)
    {
        this.trustStorePassword = trustStorePassword;
        return this;
    }
}
