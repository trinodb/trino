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
package io.trino.plugin.inference;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.net.URI;
import java.util.Optional;

import static com.google.common.net.HttpHeaders.AUTHORIZATION;

public class InferenceConfig
{
    private URI endpoint;
    private int maxBatchSize = 1000;
    private int maxRetries = 3;
    private String authenticationToken;
    private File authenticationTokenFile;
    private String authenticationHeaderName = AUTHORIZATION;

    @NotNull
    public URI getEndpoint()
    {
        return endpoint;
    }

    @Config("inference.endpoint")
    @ConfigDescription("Base URI of the Open Inference Protocol (V2) model server")
    public InferenceConfig setEndpoint(URI endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    @Min(1)
    @Max(100_000)
    public int getMaxBatchSize()
    {
        return maxBatchSize;
    }

    @Config("inference.max-batch-size")
    @ConfigDescription("Maximum number of rows sent to the model server in a single inference request")
    public InferenceConfig setMaxBatchSize(int maxBatchSize)
    {
        this.maxBatchSize = maxBatchSize;
        return this;
    }

    @Min(0)
    @Max(100)
    public int getMaxRetries()
    {
        return maxRetries;
    }

    @Config("inference.max-retries")
    @ConfigDescription("Maximum number of times a request to the model server is retried after a connection failure or a temporary error")
    public InferenceConfig setMaxRetries(int maxRetries)
    {
        this.maxRetries = maxRetries;
        return this;
    }

    public Optional<String> getAuthenticationToken()
    {
        return Optional.ofNullable(authenticationToken);
    }

    @Config("inference.authentication.token")
    @ConfigDescription("Token sent to the model server in the authentication header")
    @ConfigSecuritySensitive
    public InferenceConfig setAuthenticationToken(String authenticationToken)
    {
        this.authenticationToken = authenticationToken;
        return this;
    }

    public Optional<@FileExists File> getAuthenticationTokenFile()
    {
        return Optional.ofNullable(authenticationTokenFile);
    }

    @Config("inference.authentication.token-file")
    @ConfigDescription("File with the token sent to the model server in the authentication header. The file is read for every request, so the token can be rotated")
    public InferenceConfig setAuthenticationTokenFile(File authenticationTokenFile)
    {
        this.authenticationTokenFile = authenticationTokenFile;
        return this;
    }

    @NotEmpty
    public String getAuthenticationHeaderName()
    {
        return authenticationHeaderName;
    }

    @Config("inference.authentication.header-name")
    @ConfigDescription("Name of the HTTP header with the token. The value of the Authorization header is the token with the Bearer prefix, and the value of other headers is the token")
    public InferenceConfig setAuthenticationHeaderName(String authenticationHeaderName)
    {
        this.authenticationHeaderName = authenticationHeaderName;
        return this;
    }

    @AssertTrue(message = "Only one of inference.authentication.token and inference.authentication.token-file can be set")
    public boolean isAuthenticationTokenValid()
    {
        return authenticationToken == null || authenticationTokenFile == null;
    }
}
