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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;

import java.util.Optional;

import static org.apache.iceberg.rest.auth.OAuth2Properties.TOKEN;

/**
 * Resolves the catalog-ready token for a user session.
 * If token exchange is configured, exchanges the user's OIDC token for a new token
 * with the catalog audience before returning it. Otherwise returns the original token.
 */
class UserTokenProvider
{
    private final Optional<OidcTokenExchanger> exchanger;

    @Inject
    UserTokenProvider(Optional<OidcTokenExchanger> exchanger)
    {
        this.exchanger = exchanger;
    }

    Optional<String> catalogToken(ConnectorSession session)
    {
        return catalogToken(session.getIdentity().getExtraCredentials().get(TOKEN));
    }

    Optional<String> catalogToken(String oidcToken)
    {
        if (oidcToken == null) {
            return Optional.empty();
        }
        return Optional.of(exchanger.map(ex -> ex.getToken(oidcToken)).orElse(oidcToken));
    }
}
