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
package io.trino.server.security.jwt;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;
import io.airlift.configuration.validation.FileExists;

import java.io.File;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class JwtAuthenticatorConfig
{
    private String keyFile;
    private String requiredIssuer;
    private String requiredAudience;
    private String principalField = "sub";
    private Optional<String> userMappingPattern = Optional.empty();
    private Optional<File> userMappingFile = Optional.empty();
    private List<File> configFiles = ImmutableList.of();
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    @Deprecated
    public String getKeyFile()
    {
        return keyFile;
    }

    @Config("http-server.authentication.jwt.key-file")
    @LegacyConfig("http.authentication.jwt.key-file")
    @Deprecated
    public JwtAuthenticatorConfig setKeyFile(String keyFile)
    {
        this.keyFile = keyFile;
        return this;
    }

    @Deprecated
    public String getRequiredIssuer()
    {
        return requiredIssuer;
    }

    @Config("http-server.authentication.jwt.required-issuer")
    @LegacyConfig("http.authentication.jwt.required-issuer")
    @Deprecated
    public JwtAuthenticatorConfig setRequiredIssuer(String requiredIssuer)
    {
        this.requiredIssuer = requiredIssuer;
        return this;
    }

    @Deprecated
    public String getRequiredAudience()
    {
        return requiredAudience;
    }

    @Config("http-server.authentication.jwt.required-audience")
    @LegacyConfig("http.authentication.jwt.required-audience")
    @Deprecated
    public JwtAuthenticatorConfig setRequiredAudience(String requiredAudience)
    {
        this.requiredAudience = requiredAudience;
        return this;
    }

    @Deprecated
    public String getPrincipalField()
    {
        return principalField;
    }

    @Config("http-server.authentication.jwt.principal-field")
    @Deprecated
    public JwtAuthenticatorConfig setPrincipalField(String principalField)
    {
        this.principalField = principalField;
        return this;
    }

    @Deprecated
    public Optional<String> getUserMappingPattern()
    {
        return userMappingPattern;
    }

    @Config("http-server.authentication.jwt.user-mapping.pattern")
    @Deprecated
    public JwtAuthenticatorConfig setUserMappingPattern(String userMappingPattern)
    {
        this.userMappingPattern = Optional.ofNullable(userMappingPattern);
        return this;
    }

    public Optional<@FileExists File> getUserMappingFile()
    {
        return userMappingFile;
    }

    @Config("http-server.authentication.jwt.user-mapping.file")
    @Deprecated
    public JwtAuthenticatorConfig setUserMappingFile(File userMappingFile)
    {
        this.userMappingFile = Optional.ofNullable(userMappingFile);
        return this;
    }

    public List<@FileExists File> getJwtIdpConfigFiles()
    {
        return configFiles;
    }

    @Config("http-server.authentication.jwt.config-files")
    @ConfigDescription("JWT IDP config files")
    public JwtAuthenticatorConfig setJwtIdpConfigFiles(String configFiles)
    {
        this.configFiles = SPLITTER.splitToList(configFiles).stream()
                .map(File::new)
                .collect(toImmutableList());
        return this;
    }

    public JwtAuthenticatorConfig setJwtIdpConfigFiles(List<File> configFiles)
    {
        this.configFiles = ImmutableList.copyOf(configFiles);
        return this;
    }
}
