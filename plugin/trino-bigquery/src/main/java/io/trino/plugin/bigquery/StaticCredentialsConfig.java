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
package io.trino.plugin.bigquery;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;

import javax.validation.constraints.AssertTrue;

import java.util.Optional;

public class StaticCredentialsConfig
{
    private Optional<String> credentialsKey = Optional.empty();
    private Optional<String> credentialsFile = Optional.empty();

    @AssertTrue(message = "Exactly one of 'bigquery.credentials-key' or 'bigquery.credentials-file' must be specified")
    public boolean isCredentialsConfigurationValid()
    {
        // only one of them (at most) should be present
        return credentialsKey.isEmpty() || credentialsFile.isEmpty();
    }

    public Optional<String> getCredentialsKey()
    {
        return credentialsKey;
    }

    @Config("bigquery.credentials-key")
    @ConfigDescription("The base64 encoded credentials key")
    @ConfigSecuritySensitive
    public StaticCredentialsConfig setCredentialsKey(String credentialsKey)
    {
        this.credentialsKey = Optional.ofNullable(credentialsKey);
        return this;
    }

    public Optional<@FileExists String> getCredentialsFile()
    {
        return credentialsFile;
    }

    @Config("bigquery.credentials-file")
    @ConfigDescription("The path to the JSON credentials file")
    public StaticCredentialsConfig setCredentialsFile(String credentialsFile)
    {
        this.credentialsFile = Optional.ofNullable(credentialsFile);
        return this;
    }
}
