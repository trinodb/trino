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
package io.trino.plugin.exchange.hdfs;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.DataSize;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class ExchangeHdfsConfig
{
    private DataSize hdfsStorageBlockSize = DataSize.of(4, MEGABYTE);
    private boolean skipDirectorySchemeValidation;
    private List<File> resourceConfigFiles = ImmutableList.of();

    @NotNull
    @MinDataSize("4MB")
    @MaxDataSize("256MB")
    public DataSize getHdfsStorageBlockSize()
    {
        return hdfsStorageBlockSize;
    }

    @Config("exchange.hdfs.block-size")
    @ConfigDescription("Block size for HDFS storage")
    public ExchangeHdfsConfig setHdfsStorageBlockSize(DataSize hdfsStorageBlockSize)
    {
        this.hdfsStorageBlockSize = hdfsStorageBlockSize;
        return this;
    }

    public boolean isSkipDirectorySchemeValidation()
    {
        return skipDirectorySchemeValidation;
    }

    @Config("exchange.hdfs.skip-directory-scheme-validation")
    @ConfigDescription("Skip directory scheme validation to support hadoop compatible file system")
    public ExchangeHdfsConfig setSkipDirectorySchemeValidation(boolean skipDirectorySchemeValidation)
    {
        this.skipDirectorySchemeValidation = skipDirectorySchemeValidation;
        return this;
    }

    @NotNull
    public List<@FileExists File> getResourceConfigFiles()
    {
        return resourceConfigFiles;
    }

    @Config("hdfs.config.resources")
    public ExchangeHdfsConfig setResourceConfigFiles(List<String> files)
    {
        this.resourceConfigFiles = files.stream()
                .map(File::new)
                .collect(toImmutableList());
        return this;
    }
}
