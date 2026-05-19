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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.cache.NonEvictableCache;
import io.trino.cache.SafeCaches;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.spi.TrinoException;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleWithWebIdentityResponse;

import java.net.URI;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_CATALOG_ERROR;

public class OidcStsCredentialExchanger
{
    private static final Logger log = Logger.get(OidcStsCredentialExchanger.class);

    private static final String SESSION_NAME = "trino-iceberg";
    // Minimum STS session duration in seconds (AWS/MinIO lower bound)
    private static final int MIN_DURATION_SECONDS = 900;
    // Renew this many seconds before the STS credential actually expires
    private static final long EXPIRY_BUFFER_SECONDS = 60;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    record CachedCredentials(AwsSessionCredentials credentials, Instant expiresAt) {}

    private final StsClient stsClient;
    private final Optional<String> roleArn;
    private final String stsEndpoint;
    private final String stsRegion;
    private final Optional<String> policy;
    private final Optional<Integer> configuredDurationSeconds;
    // Keyed on the JWT `sub` claim; expiry is checked manually via CachedCredentials.expiresAt
    private final NonEvictableCache<String, CachedCredentials> cache;

    OidcStsCredentialExchanger(StsClient stsClient, String roleArn)
    {
        this.stsClient = stsClient;
        this.roleArn = Optional.of(roleArn);
        this.stsEndpoint = "(default)";
        this.stsRegion = "(default)";
        this.policy = Optional.empty();
        this.configuredDurationSeconds = Optional.empty();
        this.cache = SafeCaches.buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));
    }

    @Inject
    public OidcStsCredentialExchanger(S3FileSystemConfig s3Config, IcebergRestCatalogSigV4Config sigV4Config)
    {
        StsClientBuilder builder = StsClient.builder();

        // Prefer dedicated STS region; fall back to the general S3 region
        this.stsRegion = Optional.ofNullable(s3Config.getStsRegion())
                .or(() -> Optional.ofNullable(s3Config.getRegion()))
                .orElse("(default)");
        Optional.ofNullable(stsRegion).filter(r -> !r.equals("(default)"))
                .map(Region::of)
                .ifPresent(builder::region);

        // Required for MinIO: override the default AWS STS endpoint
        this.stsEndpoint = Optional.ofNullable(s3Config.getStsEndpoint()).orElse("(default)");
        Optional.ofNullable(s3Config.getStsEndpoint())
                .map(URI::create)
                .ifPresent(builder::endpointOverride);

        this.stsClient = builder.build();
        this.roleArn = sigV4Config.getStsRoleArn()
                .or(() -> Optional.ofNullable(s3Config.getIamRole()));
        this.policy = sigV4Config.getStsPolicy();
        this.configuredDurationSeconds = sigV4Config.getStsDurationSeconds();
        this.cache = SafeCaches.buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(1000));
        log.debug(
                "OidcStsCredentialExchanger initialized: stsEndpoint=%s, stsRegion=%s, roleArn=%s, policy=%s, durationSeconds=%s",
                stsEndpoint,
                stsRegion,
                this.roleArn,
                this.policy,
                this.configuredDurationSeconds);
    }

    /**
     * Returns STS credentials for the user, using a cache keyed on the JWT {@code sub} claim.
     * Credentials are refreshed when they are within {@value EXPIRY_BUFFER_SECONDS} seconds of expiry.
     */
    public AwsSessionCredentials getCredentials(String catalogToken)
    {
        String sub = extractClaim(catalogToken, "sub");
        if (sub == null) {
            throw new TrinoException(ICEBERG_CATALOG_ERROR, "Token is missing required 'sub' claim");
        }
        CachedCredentials cached = cache.getIfPresent(sub);
        if (cached != null && cached.expiresAt().minusSeconds(EXPIRY_BUFFER_SECONDS).isAfter(Instant.now())) {
            log.debug("STS cache hit for sub=%s, expiresAt=%s", sub, cached.expiresAt());
            return cached.credentials();
        }
        log.debug("STS cache miss for sub=%s, calling AssumeRoleWithWebIdentity", sub);
        CachedCredentials fresh = exchange(catalogToken, sub);
        cache.put(sub, fresh);
        return fresh.credentials();
    }

    CachedCredentials exchange(String catalogToken, String sub)
    {
        int durationSeconds = configuredDurationSeconds.orElseGet(() -> computeDuration(catalogToken));
        log.debug(
                "AssumeRoleWithWebIdentity request: stsEndpoint=%s, stsRegion=%s, roleArn=%s, sessionName=%s, durationSeconds=%d",
                stsEndpoint,
                stsRegion,
                roleArn,
                SESSION_NAME,
                durationSeconds);
        AssumeRoleWithWebIdentityRequest.Builder request = AssumeRoleWithWebIdentityRequest.builder()
                .webIdentityToken(catalogToken)
                .roleSessionName(SESSION_NAME)
                .durationSeconds(durationSeconds);
        roleArn.ifPresent(request::roleArn);
        policy.ifPresent(request::policy);
        AssumeRoleWithWebIdentityResponse response = stsClient.assumeRoleWithWebIdentity(request.build());

        AwsSessionCredentials credentials = AwsSessionCredentials.create(
                response.credentials().accessKeyId(),
                response.credentials().secretAccessKey(),
                response.credentials().sessionToken());
        log.debug(
                "AssumeRoleWithWebIdentity response: accessKeyId=%s, expiresAt=%s, assumedRoleId=%s, assumedRoleArn=%s, subjectFromToken=%s, audience=%s, provider=%s",
                credentials.accessKeyId(),
                response.credentials().expiration(),
                response.assumedRoleUser().assumedRoleId(),
                response.assumedRoleUser().arn(),
                response.subjectFromWebIdentityToken(),
                response.audience(),
                response.provider());

        return new CachedCredentials(credentials, response.credentials().expiration());
    }

    // Requests a STS duration equal to the remaining lifetime of the token, clamped to the STS minimum
    static int computeDuration(String jwt)
    {
        try {
            String expStr = extractClaim(jwt, "exp");
            if (expStr == null) {
                return MIN_DURATION_SECONDS;
            }
            long expEpoch = Long.parseLong(expStr);
            long remaining = expEpoch - Instant.now().getEpochSecond();
            return (int) Math.max(remaining, MIN_DURATION_SECONDS);
        }
        catch (Exception e) {
            return MIN_DURATION_SECONDS;
        }
    }

    // Decodes the JWT payload without verifying the signature — Trino already authenticated the token
    static String extractClaim(String jwt, String claim)
    {
        try {
            String[] parts = jwt.split("\\.");
            if (parts.length < 2) {
                return null;
            }
            byte[] payload = Base64.getUrlDecoder().decode(parts[1]);
            JsonNode node = MAPPER.readTree(payload);
            return node.path(claim).asText(null);
        }
        catch (Exception e) {
            return null;
        }
    }
}
