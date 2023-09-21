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

import com.google.common.base.Splitter;
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

public class PasswordAuthenticatorConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private Optional<String> userMappingPattern = Optional.empty();
    private Optional<File> userMappingFile = Optional.empty();
    private List<File> passwordAuthenticatorFiles = ImmutableList.of(new File("etc/password-authenticator.properties"));

    public Optional<String> getUserMappingPattern()
    {
        return userMappingPattern;
    }

    @Config("http-server.authentication.password.user-mapping.pattern")
    public PasswordAuthenticatorConfig setUserMappingPattern(String userMappingPattern)
    {
        this.userMappingPattern = Optional.ofNullable(userMappingPattern);
        return this;
    }

    public Optional<@FileExists File> getUserMappingFile()
    {
        return userMappingFile;
    }

    @Config("http-server.authentication.password.user-mapping.file")
    public PasswordAuthenticatorConfig setUserMappingFile(File userMappingFile)
    {
        this.userMappingFile = Optional.ofNullable(userMappingFile);
        return this;
    }

    @NotNull
    @NotEmpty(message = "At least one password authenticator config file is required")
    public List<@FileExists File> getPasswordAuthenticatorFiles()
    {
        return passwordAuthenticatorFiles;
    }

    @Config("password-authenticator.config-files")
    @ConfigDescription("Ordered list of password authenticator config files")
    public PasswordAuthenticatorConfig setPasswordAuthenticatorFiles(String passwordAuthenticatorFiles)
    {
        this.passwordAuthenticatorFiles = SPLITTER.splitToList(passwordAuthenticatorFiles).stream()
                .map(File::new)
                .collect(toImmutableList());
        return this;
    }
}
