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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.RemoteSigningConfig;
import org.apache.iceberg.rest.RemoteSigningConfigParser;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogS3RemoteSigningProperties.AUTHENTICATION_PROPERTY;
import static io.trino.plugin.iceberg.catalog.rest.IcebergRestCatalogS3RemoteSigningProperties.decodeAuthenticationProperties;
import static java.util.Objects.requireNonNull;

/**
 * Settings for remotely signing the S3 requests of one table: the signing endpoint, the headers
 * and request properties sent to it, and the OAuth properties of the signing session. The file
 * system factory derives them from the table's FileIO properties and passes them to the signer
 * provider in a single extra credential.
 */
public record IcebergRestCatalogS3RemoteSigningContext(
        URI endpoint,
        Map<String, List<String>> headers,
        Map<String, String> properties,
        Map<String, String> authentication)
{
    static final String EXTRA_CREDENTIALS_PROPERTY = "internal$iceberg_rest_s3_remote_signing";

    private static final JsonCodec<IcebergRestCatalogS3RemoteSigningContext> CODEC = jsonCodec(IcebergRestCatalogS3RemoteSigningContext.class);

    public IcebergRestCatalogS3RemoteSigningContext
    {
        requireNonNull(endpoint, "endpoint is null");
        headers = requireNonNull(headers, "headers is null").entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> ImmutableList.copyOf(entry.getValue())));
        properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        authentication = ImmutableMap.copyOf(requireNonNull(authentication, "authentication is null"));
    }

    static IcebergRestCatalogS3RemoteSigningContext fromFileIoProperties(Map<String, String> fileIoProperties)
    {
        URI endpoint = URI.create(RESTUtil.resolveEndpoint(
                requireNonNull(fileIoProperties.get(CatalogProperties.URI), "catalog URI is null"),
                requireNonNull(fileIoProperties.get(RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT), "signer endpoint is null")));
        Map<String, String> authentication = decodeAuthenticationProperties(
                requireNonNull(fileIoProperties.get(AUTHENTICATION_PROPERTY), "remote signing authentication is missing"));
        String signingConfigJson = fileIoProperties.get(RESTCatalogProperties.REMOTE_SIGNING_CONFIG);
        RemoteSigningConfig signingConfig = signingConfigJson == null ? RemoteSigningConfig.EMPTY : RemoteSigningConfigParser.fromJson(signingConfigJson);

        // Keep catalog header spelling because HTTPClient applies its defaults case-sensitively.
        Map<String, List<String>> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        RESTUtil.configHeaders(fileIoProperties).forEach((name, value) -> headers.put(name, List.of(value)));
        if (authentication.containsKey(OAuth2Properties.TOKEN) || authentication.containsKey(OAuth2Properties.CREDENTIAL)) {
            // The signing session supplies the bearer token
            headers.remove("Authorization");
        }
        headers.putAll(signingConfig.headers());
        return new IcebergRestCatalogS3RemoteSigningContext(endpoint, headers, signingConfig.properties(), authentication);
    }

    static IcebergRestCatalogS3RemoteSigningContext fromJson(String json)
    {
        return CODEC.fromJson(json);
    }

    String toJson()
    {
        return CODEC.toJson(this);
    }
}
