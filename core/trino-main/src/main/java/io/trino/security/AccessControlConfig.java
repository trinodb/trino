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
package io.trino.security;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class AccessControlConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private List<File> accessControlFiles = ImmutableList.of();

    @NotNull
    public List<@FileExists File> getAccessControlFiles()
    {
        return accessControlFiles;
    }

    @Config("access-control.config-files")
    public AccessControlConfig setAccessControlFiles(String accessControlFiles)
    {
        this.accessControlFiles = SPLITTER.splitToList(accessControlFiles).stream()
                .map(File::new)
                .collect(toImmutableList());
        return this;
    }

    public AccessControlConfig setAccessControlFiles(List<File> accessControlFiles)
    {
        this.accessControlFiles = ImmutableList.copyOf(accessControlFiles);
        return this;
    }
}
