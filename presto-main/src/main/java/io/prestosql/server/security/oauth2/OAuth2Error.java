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

import javax.annotation.Nullable;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class OAuth2Error
{
    private final String error;
    private final Optional<String> errorDescription;
    private final Optional<String> errorUri;

    @JsonCreator
    public OAuth2Error(
            @JsonProperty("error") String error,
            @JsonProperty("error_description") Optional<String> errorDescription,
            @JsonProperty("error_uri") Optional<String> errorUri)
    {
        this.error = requireNonNull(error, "error is null");
        this.errorDescription = requireNonNull(errorDescription, "errorDescription is null");
        this.errorUri = requireNonNull(errorUri, "errorUri is null");
    }

    @JsonProperty
    public String getError()
    {
        return error;
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
        return errorUri;
    }
}
