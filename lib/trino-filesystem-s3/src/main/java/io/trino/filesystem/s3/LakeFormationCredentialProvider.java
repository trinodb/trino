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
package io.trino.filesystem.s3;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.cache.NonEvictableLoadingCache;
import io.trino.spi.TrinoException;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.lakeformation.model.AccessDeniedException;
import software.amazon.awssdk.services.lakeformation.model.EntityNotFoundException;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsRequest;
import software.amazon.awssdk.services.lakeformation.model.GetTemporaryGlueTableCredentialsResponse;

import java.util.concurrent.TimeUnit;

import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.util.Objects.requireNonNull;

public class LakeFormationCredentialProvider
{
    private static final Logger log = Logger.get(LakeFormationCredentialProvider.class);

    private final LakeFormationClient lakeFormationClient;
    private final NonEvictableLoadingCache<String, AwsSessionCredentials> credentialCache;
    private final int credentialDurationSeconds;

    @Inject
    public LakeFormationCredentialProvider(S3FileSystemConfig config)
    {
        requireNonNull(config, "config is null");

        String region = config.getLakeFormationRegion();
        if (region == null) {
            region = config.getRegion();
        }

        this.lakeFormationClient = LakeFormationClient.builder()
                .region(Region.of(requireNonNull(region, "Lake Formation region must be configured via fs.s3.lake-formation.region or s3.region")))
                .build();

        Duration cacheTtl = config.getLakeFormationCredentialCacheTtl();
        this.credentialDurationSeconds = (int) config.getLakeFormationCredentialDuration().getValue(TimeUnit.SECONDS);

        this.credentialCache = buildNonEvictableCache(
                CacheBuilder.newBuilder()
                        .maximumSize(1000)
                        .expireAfterWrite((long) cacheTtl.getValue(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS),
                CacheLoader.from(this::vendCredentials));
    }

    /**
     * Returns a delegating AwsCredentialsProvider that resolves fresh credentials
     * from the cache on each invocation. This ensures long-running queries always
     * get valid credentials even when cached entries are refreshed.
     */
    public AwsCredentialsProvider getCredentialsProvider(String tableArn)
    {
        requireNonNull(tableArn, "tableArn is null");
        return () -> {
            try {
                return credentialCache.getUnchecked(tableArn);
            }
            catch (UncheckedExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof TrinoException trinoException) {
                    throw trinoException;
                }
                throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to obtain Lake Formation credentials for table: " + tableArn, cause);
            }
        };
    }

    private AwsSessionCredentials vendCredentials(String tableArn)
    {
        log.debug("Vending Lake Formation credentials for table ARN: %s", tableArn);
        try {
            GetTemporaryGlueTableCredentialsResponse response = lakeFormationClient.getTemporaryGlueTableCredentials(
                    GetTemporaryGlueTableCredentialsRequest.builder()
                            .tableArn(tableArn)
                            .durationSeconds(credentialDurationSeconds)
                            .build());

            return AwsSessionCredentials.create(
                    response.accessKeyId(),
                    response.secretAccessKey(),
                    response.sessionToken());
        }
        catch (AccessDeniedException e) {
            throw new TrinoException(PERMISSION_DENIED,
                    "Lake Formation denied access to table: " + tableArn +
                            ". Verify execution role has lakeformation:GetTemporaryGlueTableCredentials permission and table permissions are granted.",
                    e);
        }
        catch (EntityNotFoundException e) {
            throw new TrinoException(NOT_FOUND,
                    "Lake Formation table not found: " + tableArn +
                            ". Verify the Glue table exists and is registered with Lake Formation.",
                    e);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    "Failed to obtain Lake Formation credentials for table: " + tableArn,
                    e);
        }
    }
}
