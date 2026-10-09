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

import io.trino.testing.containers.Floci;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.s3.S3Client;

final class TestIcebergS3RemoteSigningRestCatalogFloci
        extends AbstractTestIcebergS3RemoteSigningRestCatalog
{
    @Override
    protected String startStorage(String bucket)
    {
        Floci floci = closeAfterClass(new Floci().withEnv("FLOCI_SERVICES_S3_ENFORCE_AUTH", "true"));
        floci.start();
        try (S3Client client = S3Client.builder()
                .applyMutation(floci::updateClient)
                .credentialsProvider(StaticCredentialsProvider.create(storageCredentials()))
                .build()) {
            client.createBucket(builder -> builder.bucket(bucket));
        }
        return floci.endpoint().toString();
    }

    @Override
    protected AwsCredentials storageCredentials()
    {
        return AwsBasicCredentials.create("test", "test");
    }

    @Test
    void testStorageRejectsUnknownAccessKey()
    {
        // Floci 1.5.32 checks access keys but does not verify SigV4 signatures.
        assertStorageRejectsCredentials(
                AwsBasicCredentials.create("unknown-access-key", "test"),
                "The AWS Access Key Id you provided does not exist in our records");
    }
}
