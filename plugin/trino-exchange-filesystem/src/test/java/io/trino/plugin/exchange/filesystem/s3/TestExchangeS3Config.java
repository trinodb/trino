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
package io.trino.plugin.exchange.filesystem.s3;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.services.s3.model.StorageClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.MINUTES;

public class TestExchangeS3Config
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ExchangeS3Config.class)
                .setS3AwsAccessKey(null)
                .setS3AwsSecretKey(null)
                .setS3IamRole(null)
                .setS3ExternalId(null)
                .setS3Region(null)
                .setS3Endpoint(null)
                .setS3MaxErrorRetries(10)
                .setS3UploadPartSize(DataSize.of(5, MEGABYTE))
                .setStorageClass(StorageClass.STANDARD)
                .setRetryMode(RetryMode.ADAPTIVE)
                .setAsyncClientConcurrency(100)
                .setAsyncClientMaxPendingConnectionAcquires(10000)
                .setConnectionAcquisitionTimeout(new Duration(1, MINUTES))
                .setS3PathStyleAccess(false)
                .setGcsJsonKeyFilePath(null)
                .setGcsJsonKey(null));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws IOException
    {
        Path jsonKeyFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("exchange.s3.aws-access-key", "access")
                .put("exchange.s3.aws-secret-key", "secret")
                .put("exchange.s3.iam-role", "roleArn")
                .put("exchange.s3.external-id", "externalId")
                .put("exchange.s3.region", "us-west-1")
                .put("exchange.s3.endpoint", "https://s3.us-east-1.amazonaws.com")
                .put("exchange.s3.max-error-retries", "8")
                .put("exchange.s3.upload.part-size", "10MB")
                .put("exchange.s3.storage-class", "REDUCED_REDUNDANCY")
                .put("exchange.s3.retry-mode", "STANDARD")
                .put("exchange.s3.async-client-concurrency", "202")
                .put("exchange.s3.async-client-max-pending-connection-acquires", "999")
                .put("exchange.s3.async-client-connection-acquisition-timeout", "5m")
                .put("exchange.s3.path-style-access", "true")
                .put("exchange.gcs.json-key-file-path", jsonKeyFile.toString())
                .put("exchange.gcs.json-key", "{}")
                .buildOrThrow();

        ExchangeS3Config expected = new ExchangeS3Config()
                .setS3AwsAccessKey("access")
                .setS3AwsSecretKey("secret")
                .setS3IamRole("roleArn")
                .setS3ExternalId("externalId")
                .setS3Region("us-west-1")
                .setS3Endpoint("https://s3.us-east-1.amazonaws.com")
                .setS3MaxErrorRetries(8)
                .setS3UploadPartSize(DataSize.of(10, MEGABYTE))
                .setStorageClass(StorageClass.REDUCED_REDUNDANCY)
                .setRetryMode(RetryMode.STANDARD)
                .setAsyncClientConcurrency(202)
                .setAsyncClientMaxPendingConnectionAcquires(999)
                .setConnectionAcquisitionTimeout(new Duration(5, MINUTES))
                .setS3PathStyleAccess(true)
                .setGcsJsonKeyFilePath(jsonKeyFile.toString())
                .setGcsJsonKey("{}");

        assertFullMapping(properties, expected);
    }
}
