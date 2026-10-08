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
package io.trino.spi.security;

import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * A token obtained through an OAuth2 token exchange (RFC 8693), usable as a {@code Bearer}
 * credential for the requested audience.
 */
public record OAuth2Token(String accessToken, Optional<Instant> expiration)
{
    public OAuth2Token
    {
        requireNonNull(accessToken, "accessToken is null");
        requireNonNull(expiration, "expiration is null");
    }

    public OAuth2Token(String accessToken)
    {
        this(accessToken, Optional.empty());
    }
}
