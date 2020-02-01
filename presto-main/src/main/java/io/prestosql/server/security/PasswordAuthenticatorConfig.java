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

public class PasswordAuthenticatorConfig
{
    private Optional<String> userExtractionPattern = Optional.empty();
    private Optional<File> userExtractionFile = Optional.empty();

    public Optional<String> getUserExtractionPattern()
    {
        return userExtractionPattern;
    }

    @Config("http-server.authentication.password.user-extraction.pattern")
    public PasswordAuthenticatorConfig setUserExtractionPattern(String userExtractionPattern)
    {
        this.userExtractionPattern = Optional.ofNullable(userExtractionPattern);
        return this;
    }

    public Optional<File> getUserExtractionFile()
    {
        return userExtractionFile;
    }

    @Config("http-server.authentication.password.user-extraction.file")
    public PasswordAuthenticatorConfig setUserExtractionFile(File userExtractionFile)
    {
        this.userExtractionFile = Optional.ofNullable(userExtractionFile);
        return this;
    }
}
