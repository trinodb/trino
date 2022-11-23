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
import io.airlift.configuration.validation.FileExists;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;

import java.util.Optional;

public class MetastoreKerberosConfig
{
    private String hiveMetastoreServicePrincipal;
    private String hiveMetastoreClientPrincipal;
    private String hiveMetastoreClientKeytab;
    private String hiveMetastoreCredentialCachePath;

    @NotNull
    public String getHiveMetastoreServicePrincipal()
    {
        return hiveMetastoreServicePrincipal;
    }

    @Config("hive.metastore.service.principal")
    @ConfigDescription("Hive Metastore service principal")
    public MetastoreKerberosConfig setHiveMetastoreServicePrincipal(String hiveMetastoreServicePrincipal)
    {
        this.hiveMetastoreServicePrincipal = hiveMetastoreServicePrincipal;
        return this;
    }

    @NotNull
    public String getHiveMetastoreClientPrincipal()
    {
        return hiveMetastoreClientPrincipal;
    }

    @Config("hive.metastore.client.principal")
    @ConfigDescription("Hive Metastore client principal")
    public MetastoreKerberosConfig setHiveMetastoreClientPrincipal(String hiveMetastoreClientPrincipal)
    {
        this.hiveMetastoreClientPrincipal = hiveMetastoreClientPrincipal;
        return this;
    }

    @NotNull
    public Optional<@FileExists String> getHiveMetastoreClientKeytab()
    {
        return Optional.ofNullable(hiveMetastoreClientKeytab);
    }

    @Config("hive.metastore.client.keytab")
    @ConfigDescription("Hive Metastore client keytab location")
    public MetastoreKerberosConfig setHiveMetastoreClientKeytab(String hiveMetastoreClientKeytab)
    {
        this.hiveMetastoreClientKeytab = hiveMetastoreClientKeytab;
        return this;
    }

    @NotNull
    public Optional<@FileExists String> getHiveMetastoreClientCredentialCacheLocation()
    {
        return Optional.ofNullable(hiveMetastoreCredentialCachePath);
    }

    @Config("hive.metastore.client.credential-cache.location")
    @ConfigDescription("Hive Metastore client credential cache location")
    public MetastoreKerberosConfig setHiveMetastoreClientCredentialCacheLocation(String hiveMetastoreCredentialCachePath)
    {
        this.hiveMetastoreCredentialCachePath = hiveMetastoreCredentialCachePath;
        return this;
    }

    @AssertTrue(message = "Exactly one of `hive.metastore.client.keytab` or `hive.metastore.client.credential-cache.location` must be specified")
    public boolean isConfigValid()
    {
        return getHiveMetastoreClientKeytab().isPresent() ^ getHiveMetastoreClientCredentialCacheLocation().isPresent();
    }
}
