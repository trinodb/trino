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
package io.prestosql.server.security;

import io.airlift.configuration.Config;

import java.io.File;
import java.util.Optional;

public class CertificateConfig
{
    private Optional<String> userMappingPattern = Optional.empty();
    private Optional<File> userMappingFile = Optional.empty();

    public Optional<String> getUserMappingPattern()
    {
        return userMappingPattern;
    }

    @Config("http-server.authentication.certificate.user-mapping.pattern")
    public CertificateConfig setUserMappingPattern(String userMappingPattern)
    {
        this.userMappingPattern = Optional.ofNullable(userMappingPattern);
        return this;
    }

    public Optional<File> getUserMappingFile()
    {
        return userMappingFile;
    }

    @Config("http-server.authentication.certificate.user-mapping.file")
    public CertificateConfig setUserMappingFile(File userMappingFile)
    {
        this.userMappingFile = Optional.ofNullable(userMappingFile);
        return this;
    }
}
