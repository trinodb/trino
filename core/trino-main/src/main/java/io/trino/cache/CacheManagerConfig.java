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
package io.trino.cache;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.validation.FileExists;

import java.io.File;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class CacheManagerConfig
{
    private List<File> cacheManagerConfigFiles = ImmutableList.of();

    public List<@FileExists File> getCacheManagerConfigFiles()
    {
        return cacheManagerConfigFiles;
    }

    @Config("cache-manager.config-files")
    public CacheManagerConfig setCacheManagerConfigFiles(String cacheManagerConfigFiles)
    {
        this.cacheManagerConfigFiles = Stream.of(cacheManagerConfigFiles.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(File::new)
                .collect(toImmutableList());
        return this;
    }
}
