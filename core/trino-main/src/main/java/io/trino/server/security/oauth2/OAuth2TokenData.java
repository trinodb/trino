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
package io.trino.server.security.oauth2;

import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class OAuth2TokenData
{
    private final String accessToken;
    private final Optional<String> refreshToken;
    private final Instant validUntil;

    public OAuth2TokenData(String accessToken, Optional<String> refreshToken, Instant validUntil)
    {
        this.accessToken = requireNonNull(accessToken, "accessToken is null");
        this.refreshToken = requireNonNull(refreshToken, "refreshToken is null");
        this.validUntil = requireNonNull(validUntil, "validUntil is null");
    }

    public String getAccessToken()
    {
        return accessToken;
    }

    public Optional<String> getRefreshToken()
    {
        return refreshToken;
    }

    public Instant getValidUntil()
    {
        return validUntil;
    }
}
