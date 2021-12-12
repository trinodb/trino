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

import java.net.URI;
import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface OAuth2Client
{
    URI getAuthorizationUri(String state, URI callbackUri, Optional<String> nonceHash);

    OAuth2Response getOAuth2Response(String code, URI callbackUri)
            throws ChallengeFailedException;

    class OAuth2Response
    {
        private final String accessToken;
        private final Optional<Instant> validUntil;
        private final Optional<String> idToken;

        public OAuth2Response(String accessToken, Optional<Instant> validUntil, Optional<String> idToken)
        {
            this.accessToken = requireNonNull(accessToken, "accessToken is null");
            this.validUntil = requireNonNull(validUntil, "validUntil is null");
            this.idToken = requireNonNull(idToken, "idToken is null");
        }

        public String getAccessToken()
        {
            return accessToken;
        }

        public Optional<Instant> getValidUntil()
        {
            return validUntil;
        }

        public Optional<String> getIdToken()
        {
            return idToken;
        }
    }
}
