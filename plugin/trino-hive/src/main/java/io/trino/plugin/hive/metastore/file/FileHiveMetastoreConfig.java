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
package io.trino.plugin.hive.metastore.file;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;

import javax.validation.constraints.NotNull;

import static io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig.VersionCompatibility.NOT_SUPPORTED;

@DefunctConfig("hive.metastore.assume-canonical-partition-keys")
public class FileHiveMetastoreConfig
{
    public static final String VERSION_COMPATIBILITY_CONFIG = "hive.metastore.version-compatibility";

    public enum VersionCompatibility
    {
        NOT_SUPPORTED,
        UNSAFE_ASSUME_COMPATIBILITY,
    }

    private String catalogDirectory;
    private VersionCompatibility versionCompatibility = NOT_SUPPORTED;
    private boolean disableLocationChecks; // TODO this should probably be true by default, to align with well-behaving metastores other than HMS
    private String metastoreUser = "presto";

    @NotNull
    public String getCatalogDirectory()
    {
        return catalogDirectory;
    }

    @Config("hive.metastore.catalog.dir")
    @ConfigDescription("Hive file-based metastore catalog directory")
    public FileHiveMetastoreConfig setCatalogDirectory(String catalogDirectory)
    {
        this.catalogDirectory = catalogDirectory;
        return this;
    }

    @NotNull
    public VersionCompatibility getVersionCompatibility()
    {
        return versionCompatibility;
    }

    @Config(VERSION_COMPATIBILITY_CONFIG)
    public FileHiveMetastoreConfig setVersionCompatibility(VersionCompatibility versionCompatibility)
    {
        this.versionCompatibility = versionCompatibility;
        return this;
    }

    public boolean isDisableLocationChecks()
    {
        return disableLocationChecks;
    }

    @Config("hive.metastore.disable-location-checks")
    public FileHiveMetastoreConfig setDisableLocationChecks(boolean disableLocationChecks)
    {
        this.disableLocationChecks = disableLocationChecks;
        return this;
    }

    @NotNull
    public String getMetastoreUser()
    {
        return metastoreUser;
    }

    @Config("hive.metastore.user")
    @ConfigDescription("Hive file-based metastore username for file access")
    public FileHiveMetastoreConfig setMetastoreUser(String metastoreUser)
    {
        this.metastoreUser = metastoreUser;
        return this;
    }
}
