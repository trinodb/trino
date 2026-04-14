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

import org.apache.iceberg.rest.credentials.Credential;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.iceberg.gcp.GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED;
import static org.apache.iceberg.gcp.GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT;
import static org.apache.iceberg.gcp.GCPProperties.GCS_OAUTH2_TOKEN;
import static org.apache.iceberg.gcp.GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT;

final class GcsVendedCredentialsProvider
        extends AbstractIcebergRestVendedCredentialsProvider<GcsVendedCredentials>
{
    GcsVendedCredentialsProvider(
            Map<String, String> catalogProperties,
            Map<String, String> fileIoProperties)
    {
        super(
                catalogProperties,
                fileIoProperties,
                parseBoolean(fileIoProperties, GCS_OAUTH2_REFRESH_CREDENTIALS_ENABLED, true),
                Optional.ofNullable(fileIoProperties.get(GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT)),
                createVendedCredentials(fileIoProperties));
    }

    @Override
    protected GcsVendedCredentials applyRefreshedCredentials(List<Credential> credentials)
    {
        Credential gcsCredential = credentials.stream()
                .filter(credential -> credential.prefix().startsWith("gs"))
                .reduce((_, _) -> {
                    throw new IllegalStateException("Multiple GCS credentials returned");
                })
                .orElseThrow(() -> new IllegalStateException("No GCS credentials in refresh response"));

        Map<String, String> config = gcsCredential.config();
        return createVendedCredentials(config);
    }

    private static GcsVendedCredentials createVendedCredentials(Map<String, String> fileIoProperties)
    {
        return new GcsVendedCredentials(
                fileIoProperties.get(GCS_OAUTH2_TOKEN),
                parseInstantEpochMillis(fileIoProperties, GCS_OAUTH2_TOKEN_EXPIRES_AT));
    }
}
