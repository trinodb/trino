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
package io.trino.hdfs.authentication;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;
import io.airlift.configuration.validation.FileExists;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;

import java.util.Optional;

public class HdfsKerberosConfig
{
    private String hdfsTrinoPrincipal;
    private String hdfsTrinoKeytab;
    private String hdfsTrinoCredentialCacheLocation;

    @NotNull
    public String getHdfsTrinoPrincipal()
    {
        return hdfsTrinoPrincipal;
    }

    @Config("hive.hdfs.trino.principal")
    @LegacyConfig("hive.hdfs.presto.principal")
    @ConfigDescription("Trino principal used to access HDFS")
    public HdfsKerberosConfig setHdfsTrinoPrincipal(String hdfsTrinoPrincipal)
    {
        this.hdfsTrinoPrincipal = hdfsTrinoPrincipal;
        return this;
    }

    @NotNull
    public Optional<@FileExists String> getHdfsTrinoKeytab()
    {
        return Optional.ofNullable(hdfsTrinoKeytab);
    }

    @Config("hive.hdfs.trino.keytab")
    @LegacyConfig("hive.hdfs.presto.keytab")
    @ConfigDescription("Trino keytab used to access HDFS")
    public HdfsKerberosConfig setHdfsTrinoKeytab(String hdfsTrinoKeytab)
    {
        this.hdfsTrinoKeytab = hdfsTrinoKeytab;
        return this;
    }

    @NotNull
    public Optional<@FileExists String> getHdfsTrinoCredentialCacheLocation()
    {
        return Optional.ofNullable(hdfsTrinoCredentialCacheLocation);
    }

    @Config("hive.hdfs.trino.credential-cache.location")
    @ConfigDescription("Trino credential-cache location used to access HDFS")
    public HdfsKerberosConfig setHdfsTrinoCredentialCacheLocation(String hdfsTrinoCredentialCacheLocation)
    {
        this.hdfsTrinoCredentialCacheLocation = hdfsTrinoCredentialCacheLocation;
        return this;
    }

    @AssertTrue(message = "Exactly one of `hive.hdfs.trino.keytab` or `hive.hdfs.trino.credential-cache.location` must be specified")
    public boolean isConfigValid()
    {
        return getHdfsTrinoKeytab().isPresent() ^ getHdfsTrinoCredentialCacheLocation().isPresent();
    }
}
