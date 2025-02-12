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
import com.google.inject.Inject;
import io.trino.filesystem.s3.S3FileSystemConfig;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class SigV4AwsProperties
        implements AwsProperties
{
    private final Map<String, String> properties;

    @Inject
    public SigV4AwsProperties(IcebergRestCatalogSigV4Config sigV4Config, S3FileSystemConfig s3Config)
    {
        this.properties = ImmutableMap.<String, String>builder()
                .put("rest.sigv4-enabled", "true")
                .put("rest.signing-name", sigV4Config.getSigningName())
                .put("rest.access-key-id", requireNonNull(s3Config.getAwsAccessKey(), "s3.aws-access-key is null"))
                .put("rest.secret-access-key", requireNonNull(s3Config.getAwsSecretKey(), "s3.aws-secret-key is null"))
                .put("rest.signing-region", requireNonNull(s3Config.getRegion(), "s3.region is null"))
                .put("rest-metrics-reporting-enabled", "false")
                .buildOrThrow();
    }

    @Override
    public Map<String, String> get()
    {
        return properties;
    }
}
