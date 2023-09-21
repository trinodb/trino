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
package io.trino.plugin.jdbc.credential.file;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.NotNull;

public class ConfigFileBasedCredentialProviderConfig
{
    private String credentialsFile;

    @Config("connection-credential-file")
    @ConfigDescription("Location of the file where credentials are present")
    public ConfigFileBasedCredentialProviderConfig setCredentialFile(String connectionUser)
    {
        this.credentialsFile = connectionUser;
        return this;
    }

    @NotNull
    @FileExists
    public String getCredentialFile()
    {
        return credentialsFile;
    }
}
