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

import io.trino.testing.containers.Minio;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;

import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;

final class TestIcebergS3RemoteSigningRestCatalog
        extends AbstractTestIcebergS3RemoteSigningRestCatalog
{
    @Override
    protected String startStorage(String bucket)
    {
        Minio minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket(bucket);
        return minio.getMinioAddress();
    }

    @Override
    protected AwsCredentials storageCredentials()
    {
        return AwsBasicCredentials.create(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD);
    }

    @Test
    void testStorageRejectsInvalidSignature()
    {
        assertStorageRejectsCredentials(
                AwsBasicCredentials.create(MINIO_ROOT_USER, "incorrect-secret"),
                "The request signature we calculated does not match");
    }
}
