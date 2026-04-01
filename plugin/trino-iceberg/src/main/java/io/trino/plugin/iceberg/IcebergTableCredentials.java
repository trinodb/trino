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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.ConnectorTableCredentials;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.FileIO;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public record IcebergTableCredentials(Map<String, String> fileIoProperties, Optional<Instant> expiresAt)
        implements ConnectorTableCredentials
{
    // S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS is package-private in Iceberg,
    // so we duplicate the string here. See apache/iceberg#11389.
    private static final String S3_SESSION_TOKEN_EXPIRES_AT_MS = "s3.session-token-expires-at-ms";
    // Default credential TTL when vended credentials are present but no expiry timestamp
    // property is found. Matches the default AWS STS session token lifetime (1 hour).
    private static final long DEFAULT_CREDENTIAL_TTL_MINUTES = 60;
    // Prefetch buffer before credential expiry, matching Iceberg's VendedCredentialsProvider pattern.
    private static final long PREFETCH_BUFFER_MINUTES = 5;

    public IcebergTableCredentials
    {
        fileIoProperties = ImmutableMap.copyOf(fileIoProperties);
        requireNonNull(expiresAt, "expiresAt is null");
    }

    public boolean shouldRefresh()
    {
        return expiresAt.map(expiry -> Instant.now().isAfter(expiry)).orElse(false);
    }

    public static IcebergTableCredentials forFileIO(FileIO io)
    {
        return forProperties(io.properties());
    }

    static IcebergTableCredentials forProperties(Map<String, String> properties)
    {
        // Only compute an expiry when actual vended credentials are present.
        // Empty expiresAt means credentials never expire, so catalogs that do not vend
        // credentials are unaffected even if getTableCredentials() is called.
        if (!hasVendedCredentials(properties)) {
            return new IcebergTableCredentials(properties, Optional.empty());
        }
        // Extract the actual credential expiry from the properties, following the pattern in
        // Iceberg's VendedCredentialsProvider (apache/iceberg#11389, #11577, #11282).
        // Prefetch PREFETCH_BUFFER_MINUTES before actual expiry so shouldRefresh() triggers
        // proactive refresh on lazy access (CachingKerberosAuthentication pattern).
        OptionalLong expiresAtMs = extractExpiresAtMs(properties);
        Instant expiresAt = expiresAtMs.isPresent()
                ? Instant.ofEpochMilli(expiresAtMs.getAsLong()).minus(PREFETCH_BUFFER_MINUTES, ChronoUnit.MINUTES)
                : Instant.now().plus(DEFAULT_CREDENTIAL_TTL_MINUTES - PREFETCH_BUFFER_MINUTES, ChronoUnit.MINUTES);
        return new IcebergTableCredentials(properties, Optional.of(expiresAt));
    }

    private static OptionalLong extractExpiresAtMs(Map<String, String> properties)
    {
        // S3: s3.session-token-expires-at-ms (apache/iceberg#11389)
        String s3Expiry = properties.get(S3_SESSION_TOKEN_EXPIRES_AT_MS);
        if (s3Expiry != null) {
            return OptionalLong.of(Long.parseLong(s3Expiry));
        }
        // GCS: gcs.oauth2.token-expires-at (apache/iceberg#11282)
        String gcsExpiry = properties.get(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT);
        if (gcsExpiry != null) {
            return OptionalLong.of(Long.parseLong(gcsExpiry));
        }
        // Azure: adls.sas-token-expires-at-ms.<account> — use earliest expiry across all accounts
        OptionalLong azureExpiry = properties.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX))
                .mapToLong(entry -> Long.parseLong(entry.getValue()))
                .min();
        if (azureExpiry.isPresent()) {
            return azureExpiry;
        }
        return OptionalLong.empty();
    }

    private static boolean hasVendedCredentials(Map<String, String> properties)
    {
        return properties.containsKey(S3FileIOProperties.ACCESS_KEY_ID) ||
                properties.containsKey(GCPProperties.GCS_OAUTH2_TOKEN) ||
                properties.keySet().stream().anyMatch(key -> key.startsWith(AzureProperties.ADLS_SAS_TOKEN_PREFIX));
    }
}
