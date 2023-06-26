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
package io.trino.server.security;

import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class HeaderAuthenticatorConfig
{
    private Optional<String> userMappingPattern = Optional.empty();
    private Optional<File> userMappingFile = Optional.empty();
    private List<File> headerAuthenticatorFiles = ImmutableList.of(new File("etc/header-authenticator.properties"));

    @Config("http-server.authentication.header.user-mapping.pattern")
    @ConfigDescription("An optional user mapping pattern to be applied to the authenticated principal")
    public HeaderAuthenticatorConfig setUserMappingPattern(String userMappingPattern)
    {
        this.userMappingPattern = Optional.ofNullable(userMappingPattern);

        return this;
    }

    public Optional<String> getUserMappingPattern()
    {
        return this.userMappingPattern;
    }

    @Config("http-server.authentication.header.user-mapping.file")
    @ConfigDescription("An optional user mapping file containing the mapping rules to be applied to the authenticated principal")
    public HeaderAuthenticatorConfig setUserMappingFile(File userMappingFile)
    {
        this.userMappingFile = Optional.ofNullable(userMappingFile);

        return this;
    }

    public Optional<@FileExists File> getUserMappingFile()
    {
        return this.userMappingFile;
    }

    @Config("header-authenticator.config-files")
    @ConfigDescription("Ordered list of header authenticator configuration files")
    public HeaderAuthenticatorConfig setHeaderAuthenticatorFiles(List<String> headerAuthenticatorFiles)
    {
        this.headerAuthenticatorFiles = headerAuthenticatorFiles.stream()
                .map(File::new)
                .collect(toImmutableList());

        return this;
    }

    @NotNull
    @NotEmpty(message = "At least one header authenticator config file is required")
    public List<@FileExists File> getHeaderAuthenticatorFiles()
    {
        return this.headerAuthenticatorFiles;
    }
}
