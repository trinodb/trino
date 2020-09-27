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
package io.prestosql.server.security.oauth2;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.scribejava.core.oauth2.OAuth2Error;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Error response according to: https://tools.ietf.org/html/rfc6749#section-4.1.2.1
 */
public class OAuth2ErrorResponse
{
    private final OAuth2Error error;
    private final Optional<String> errorDescription;
    private final Optional<URI> errorUri;

    @JsonCreator
    public OAuth2ErrorResponse(
            @JsonProperty("error") OAuth2Error error,
            @JsonProperty("error_description") Optional<String> errorDescription,
            @JsonProperty("error_uri") Optional<URI> errorUri)
    {
        this.error = requireNonNull(error, "error is null");
        this.errorDescription = requireNonNull(errorDescription, "errorDescription is null");
        this.errorUri = requireNonNull(errorUri, "errorUri is null");
    }

    @JsonProperty
    public String getError()
    {
        return error.getErrorString();
    }

    @JsonProperty("error_description")
    public Optional<String> getErrorDescription()
    {
        return errorDescription;
    }

    @JsonProperty("error_uri")
    @Nullable
    public Optional<String> getErrorUri()
    {
        return errorUri.map(URI::toString);
    }
}
