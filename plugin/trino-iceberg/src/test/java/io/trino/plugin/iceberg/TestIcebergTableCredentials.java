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
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TestIcebergTableCredentials
{
    @Test
    void testNoVendedCredentials()
    {
        Map<String, String> properties = ImmutableMap.of("warehouse", "s3://bucket/path");
        IcebergTableCredentials credentials = IcebergTableCredentials.forProperties(properties);

        assertThat(credentials.expiresAt()).isEmpty();
        assertThat(credentials.shouldRefresh()).isFalse();
    }

    @Test
    void testS3VendedCredentialsWithExpiry()
    {
        Instant actualExpiry = Instant.now().plus(Duration.ofMinutes(30));
        Map<String, String> properties = ImmutableMap.of(
                S3FileIOProperties.ACCESS_KEY_ID, "FAKE_ACCESS_KEY",
                S3FileIOProperties.SECRET_ACCESS_KEY, "FAKE_SECRET_KEY",
                S3FileIOProperties.SESSION_TOKEN, "FAKE_SESSION_TOKEN",
                "s3.session-token-expires-at-ms", Long.toString(actualExpiry.toEpochMilli()));

        IcebergTableCredentials credentials = IcebergTableCredentials.forProperties(properties);

        // expiresAt should be 5 minutes before the actual expiry (prefetch buffer)
        assertThat(credentials.expiresAt()).isPresent();
        assertThat(credentials.expiresAt().get()).isBefore(actualExpiry);
        assertThat(credentials.expiresAt().get()).isAfter(Instant.now().plus(Duration.ofMinutes(20)));
        assertThat(credentials.shouldRefresh()).isFalse();
    }

    @Test
    void testS3VendedCredentialsWithoutExpiry()
    {
        Map<String, String> properties = ImmutableMap.of(
                S3FileIOProperties.ACCESS_KEY_ID, "FAKE_ACCESS_KEY",
                S3FileIOProperties.SECRET_ACCESS_KEY, "FAKE_SECRET_KEY",
                S3FileIOProperties.SESSION_TOKEN, "FAKE_SESSION_TOKEN");

        Instant before = Instant.now();
        IcebergTableCredentials credentials = IcebergTableCredentials.forProperties(properties);
        Instant after = Instant.now();

        // Falls back to default TTL (60 min) minus prefetch buffer (5 min) = ~55 minutes
        assertThat(credentials.expiresAt()).isPresent();
        assertThat(credentials.expiresAt().get()).isAfter(before.plus(Duration.ofMinutes(54)));
        assertThat(credentials.expiresAt().get()).isBefore(after.plus(Duration.ofMinutes(56)));
        assertThat(credentials.shouldRefresh()).isFalse();
    }

    @Test
    void testGcsVendedCredentialsWithExpiry()
    {
        Instant actualExpiry = Instant.now().plus(Duration.ofMinutes(45));
        Map<String, String> properties = ImmutableMap.of(
                GCPProperties.GCS_OAUTH2_TOKEN, "ya29.test-token",
                GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, Long.toString(actualExpiry.toEpochMilli()));

        IcebergTableCredentials credentials = IcebergTableCredentials.forProperties(properties);

        assertThat(credentials.expiresAt()).isPresent();
        assertThat(credentials.expiresAt().get()).isBefore(actualExpiry);
        assertThat(credentials.expiresAt().get()).isAfter(Instant.now().plus(Duration.ofMinutes(35)));
        assertThat(credentials.shouldRefresh()).isFalse();
    }

    @Test
    void testGcsVendedCredentialsWithoutExpiry()
    {
        Map<String, String> properties = ImmutableMap.of(
                GCPProperties.GCS_OAUTH2_TOKEN, "ya29.test-token");

        Instant before = Instant.now();
        IcebergTableCredentials credentials = IcebergTableCredentials.forProperties(properties);

        assertThat(credentials.expiresAt()).isPresent();
        assertThat(credentials.expiresAt().get()).isAfter(before.plus(Duration.ofMinutes(54)));
        assertThat(credentials.shouldRefresh()).isFalse();
    }

    @Test
    void testFileIoPropertiesPreservedThroughForProperties()
    {
        Map<String, String> properties = ImmutableMap.of(
                S3FileIOProperties.ACCESS_KEY_ID, "FAKE_KEY",
                S3FileIOProperties.SECRET_ACCESS_KEY, "FAKE_SECRET",
                S3FileIOProperties.SESSION_TOKEN, "FAKE_TOKEN",
                "s3.session-token-expires-at-ms", Long.toString(Instant.now().plus(Duration.ofMinutes(30)).toEpochMilli()),
                "s3.endpoint", "https://s3.example.com");

        IcebergTableCredentials credentials = IcebergTableCredentials.forProperties(properties);

        assertThat(credentials.fileIoProperties()).containsAllEntriesOf(properties);
        assertThat(credentials.fileIoProperties()).hasSize(properties.size());
    }

    @Test
    void testS3ExpiryPrefetchBufferIsExactlyFiveMinutes()
    {
        Instant actualExpiry = Instant.ofEpochMilli(Instant.now().plus(Duration.ofMinutes(30)).toEpochMilli());
        Map<String, String> properties = ImmutableMap.of(
                S3FileIOProperties.ACCESS_KEY_ID, "KEY",
                "s3.session-token-expires-at-ms", Long.toString(actualExpiry.toEpochMilli()));

        IcebergTableCredentials credentials = IcebergTableCredentials.forProperties(properties);

        Instant expected = actualExpiry.minus(Duration.ofMinutes(5));
        assertThat(credentials.expiresAt()).isEqualTo(Optional.of(expected));
    }

    @Test
    void testShouldRefreshWithExpiredCredentials()
    {
        IcebergTableCredentials expired = new IcebergTableCredentials(
                ImmutableMap.of(S3FileIOProperties.ACCESS_KEY_ID, "expired-key"),
                Optional.of(Instant.now().minus(Duration.ofMinutes(1))));

        assertThat(expired.shouldRefresh()).isTrue();
    }

    @Test
    void testShouldRefreshWithFreshCredentials()
    {
        IcebergTableCredentials fresh = new IcebergTableCredentials(
                ImmutableMap.of(S3FileIOProperties.ACCESS_KEY_ID, "fresh-key"),
                Optional.of(Instant.now().plus(Duration.ofMinutes(25))));

        assertThat(fresh.shouldRefresh()).isFalse();
    }

    @Test
    void testShouldRefreshWithEmptyExpiry()
    {
        IcebergTableCredentials nonVended = new IcebergTableCredentials(
                ImmutableMap.of("warehouse", "s3://bucket"),
                Optional.empty());

        assertThat(nonVended.shouldRefresh()).isFalse();
    }

    @Test
    void testAzureVendedCredentialsWithExpiry()
    {
        Instant actualExpiry = Instant.now().plus(Duration.ofMinutes(40));
        Map<String, String> properties = ImmutableMap.of(
                AzureProperties.ADLS_SAS_TOKEN_PREFIX + "mystorageaccount", "sv=2022-test-sas-token",
                AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + "mystorageaccount", Long.toString(actualExpiry.toEpochMilli()));

        IcebergTableCredentials credentials = IcebergTableCredentials.forProperties(properties);

        assertThat(credentials.expiresAt()).isPresent();
        assertThat(credentials.expiresAt().get()).isBefore(actualExpiry);
        assertThat(credentials.expiresAt().get()).isAfter(Instant.now().plus(Duration.ofMinutes(30)));
        assertThat(credentials.shouldRefresh()).isFalse();
    }

    @Test
    void testAzureVendedCredentialsWithoutExpiry()
    {
        Map<String, String> properties = ImmutableMap.of(
                AzureProperties.ADLS_SAS_TOKEN_PREFIX + "mystorageaccount", "sv=2022-test-sas-token");

        Instant before = Instant.now();
        IcebergTableCredentials credentials = IcebergTableCredentials.forProperties(properties);

        assertThat(credentials.expiresAt()).isPresent();
        assertThat(credentials.expiresAt().get()).isAfter(before.plus(Duration.ofMinutes(54)));
        assertThat(credentials.shouldRefresh()).isFalse();
    }

    @Test
    void testAzureMultipleAccountsUsesEarliestExpiry()
    {
        Instant earlierExpiry = Instant.ofEpochMilli(Instant.now().plus(Duration.ofMinutes(20)).toEpochMilli());
        Instant laterExpiry = Instant.ofEpochMilli(Instant.now().plus(Duration.ofMinutes(50)).toEpochMilli());
        Map<String, String> properties = ImmutableMap.of(
                AzureProperties.ADLS_SAS_TOKEN_PREFIX + "account1", "sas-token-1",
                AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + "account1", Long.toString(earlierExpiry.toEpochMilli()),
                AzureProperties.ADLS_SAS_TOKEN_PREFIX + "account2", "sas-token-2",
                AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + "account2", Long.toString(laterExpiry.toEpochMilli()));

        IcebergTableCredentials credentials = IcebergTableCredentials.forProperties(properties);

        // Should use the earliest expiry (account1) minus prefetch buffer
        assertThat(credentials.expiresAt()).isPresent();
        Instant expected = earlierExpiry.minus(Duration.ofMinutes(5));
        assertThat(credentials.expiresAt().get()).isEqualTo(expected);
    }
}
