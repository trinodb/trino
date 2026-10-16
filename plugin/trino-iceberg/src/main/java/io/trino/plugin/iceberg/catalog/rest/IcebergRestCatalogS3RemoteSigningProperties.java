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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.json.JsonCodec;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static io.airlift.json.JsonCodec.mapJsonCodec;

final class IcebergRestCatalogS3RemoteSigningProperties
{
    static final String AUTHENTICATION_PROPERTY = "internal$iceberg_rest_s3_signer_authentication$";

    private static final JsonCodec<Map<String, String>> AUTHENTICATION_CODEC = mapJsonCodec(String.class, String.class);
    private static final String HEADER_PREFIX = "header.";
    // The catalog URI separates session keys when catalogs share an OAuth endpoint.
    private static final Set<String> CATALOG_AUTHENTICATION_PROPERTIES = ImmutableSet.of(
            CatalogProperties.URI,
            OAuth2Properties.TOKEN,
            OAuth2Properties.CREDENTIAL,
            OAuth2Properties.TOKEN_EXPIRES_IN_MS,
            OAuth2Properties.SCOPE,
            OAuth2Properties.OAUTH2_SERVER_URI,
            OAuth2Properties.TOKEN_REFRESH_ENABLED,
            OAuth2Properties.TOKEN_EXCHANGE_ENABLED,
            OAuth2Properties.AUDIENCE,
            OAuth2Properties.RESOURCE);
    private static final Set<String> TABLE_AUTHENTICATION_PROPERTIES = ImmutableSet.of(
            OAuth2Properties.TOKEN,
            OAuth2Properties.CREDENTIAL,
            OAuth2Properties.TOKEN_EXPIRES_IN_MS);

    private IcebergRestCatalogS3RemoteSigningProperties() {}

    private static Map<String, String> authenticationProperties(Map<String, String> catalogProperties, Map<String, String> tableProperties)
    {
        Map<String, String> properties = new HashMap<>(Maps.filterKeys(catalogProperties, key -> CATALOG_AUTHENTICATION_PROPERTIES.contains(key) || key.startsWith(HEADER_PREFIX)));
        properties.putAll(OAuth2Util.buildOptionalParam(catalogProperties, "sign"));
        properties.put(OAuth2Properties.OAUTH2_SERVER_URI, RESTUtil.resolveEndpoint(
                catalogProperties.get(CatalogProperties.URI),
                catalogProperties.getOrDefault(OAuth2Properties.OAUTH2_SERVER_URI, ResourcePaths.tokens())));
        if (tableProperties.containsKey(OAuth2Properties.TOKEN)) {
            properties.remove(OAuth2Properties.CREDENTIAL);
            properties.remove(OAuth2Properties.TOKEN_EXPIRES_IN_MS);
        }
        else if (tableProperties.containsKey(OAuth2Properties.CREDENTIAL) || !properties.getOrDefault(OAuth2Properties.CREDENTIAL, "").isEmpty()) {
            // A client credential takes precedence over a configured token. The catalog session works
            // the same way: it fetches its token with the credential during initialization.
            properties.remove(OAuth2Properties.TOKEN);
            properties.remove(OAuth2Properties.TOKEN_EXPIRES_IN_MS);
        }
        for (String property : TABLE_AUTHENTICATION_PROPERTIES) {
            String value = tableProperties.get(property);
            if (value != null) {
                properties.put(property, value);
            }
        }
        return ImmutableMap.copyOf(properties);
    }

    static String encodeAuthenticationProperties(Map<String, String> catalogProperties, Map<String, String> tableProperties, AuthSession catalogSession)
    {
        Map<String, String> properties = new TreeMap<>(authenticationProperties(catalogProperties, tableProperties));
        boolean usesCatalogToken = !tableProperties.containsKey(OAuth2Properties.TOKEN) &&
                !tableProperties.containsKey(OAuth2Properties.CREDENTIAL) &&
                properties.containsKey(OAuth2Properties.TOKEN);
        // The catalog properties hold the configured token, which the catalog session may have refreshed since
        if (usesCatalogToken && catalogSession instanceof OAuth2Util.AuthSession oauthSession && oauthSession.token() != null) {
            properties.put(OAuth2Properties.TOKEN, oauthSession.token());
            // The configured lifetime applies to the configured token
            properties.remove(OAuth2Properties.TOKEN_EXPIRES_IN_MS);
        }
        return AUTHENTICATION_CODEC.toJson(properties);
    }

    static Map<String, String> decodeAuthenticationProperties(String properties)
    {
        return ImmutableMap.copyOf(AUTHENTICATION_CODEC.fromJson(properties));
    }
}
