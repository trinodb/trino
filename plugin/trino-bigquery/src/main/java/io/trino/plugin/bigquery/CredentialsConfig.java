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

import com.google.auth.oauth2.GoogleCredentials;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;

import javax.validation.constraints.AssertTrue;

import java.io.IOException;
import java.util.Optional;

public class CredentialsConfig
{
    public static final String OAUTH_TOKEN_KEY = "bigquery.oauth";
    private Optional<String> credentialsKey = Optional.empty();
    private Optional<String> credentialsFile = Optional.empty();
    private boolean useAccessToken;

    @AssertTrue(message = "Exactly one of 'bigquery.credentials-key' or 'bigquery.credentials-file' must be specified, or the default GoogleCredentials could be created")
    public boolean isCredentialsConfigurationValid()
    {
        // only one of them (at most) should be present
        if (credentialsKey.isPresent() && credentialsFile.isPresent()) {
            return false;
        }
        // if no credentials were supplied, let's check if we can create the default ones
        if (credentialsKey.isEmpty() && credentialsFile.isEmpty()) {
            try {
                GoogleCredentials.getApplicationDefault();
            }
            catch (IOException e) {
                return false;
            }
        }
        return true;
    }

    public Optional<String> getCredentialsKey()
    {
        return credentialsKey;
    }

    @Config("bigquery.credentials-key")
    @ConfigDescription("The base64 encoded credentials key")
    @ConfigSecuritySensitive
    public CredentialsConfig setCredentialsKey(String credentialsKey)
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
    public CredentialsConfig setCredentialsFile(String credentialsFile)
    {
        this.credentialsFile = Optional.ofNullable(credentialsFile);
        return this;
    }

    public boolean isUseAccessToken()
    {
        return useAccessToken;
    }

    @Config("bigquery.use-access-token")
    @ConfigDescription("Use client-provided OAuth token to access Google BigQuery")
    public CredentialsConfig setUseAccessToken(boolean useAccessToken)
    {
        this.useAccessToken = useAccessToken;
        return this;
    }
}
