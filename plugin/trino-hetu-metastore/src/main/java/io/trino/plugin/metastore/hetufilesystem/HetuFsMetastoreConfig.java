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
package io.trino.plugin.metastore.hetufilesystem;

import io.airlift.configuration.Config;

import jakarta.validation.constraints.NotNull;

public class HetuFsMetastoreConfig
{
    private String hetuFileSystemMetastorePath;
    private String hetuFileSystemMetastoreProfileName;

    @NotNull
    public String getHetuFileSystemMetastorePath()
    {
        return hetuFileSystemMetastorePath;
    }

    @Config("trino.metastore.msfilesystem.path")
    public HetuFsMetastoreConfig setHetuFileSystemMetastorePath(String hetuFileSystemMetastorePath)
    {
        this.hetuFileSystemMetastorePath = hetuFileSystemMetastorePath;
        return this;
    }

    @NotNull
    public String getHetuFileSystemMetastoreProfileName()
    {
        return hetuFileSystemMetastoreProfileName;
    }

    @Config("trino.metastore.msfilesystem.profile-name")
    public HetuFsMetastoreConfig setHetuFileSystemMetastoreProfileName(String hetuFileSystemMetastoreProfileName)
    {
        this.hetuFileSystemMetastoreProfileName = hetuFileSystemMetastoreProfileName;
        return this;
    }
}
