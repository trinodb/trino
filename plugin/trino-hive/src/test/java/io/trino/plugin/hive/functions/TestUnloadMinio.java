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
package io.trino.plugin.hive.functions;

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.containers.Minio;

import java.io.IOException;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;

public class TestUnloadMinio
        extends BaseUnloadFileSystemTest
{
    private static final String bucketName = "test-bucket-" + randomNameSuffix();

    private Minio minio;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        minio = closeAfterClass(Minio.builder().build());
        minio.start();
        minio.createBucket(bucketName);
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                        .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                        .put("hive.s3.endpoint", minio.getMinioAddress())
                        .put("hive.s3.path-style-access", "true")
                        .buildOrThrow())
                .build();
    }

    @Override
    protected TrinoFileSystemFactory getFileSystemFactory()
            throws IOException
    {
        return new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setRegion(MINIO_REGION)
                        .setAwsAccessKey(MINIO_ACCESS_KEY)
                        .setAwsSecretKey(MINIO_SECRET_KEY)
                        .setEndpoint(minio.getMinioAddress())
                        .setPathStyleAccess(true));
    }

    @Override
    protected String getLocation(String path)
    {
        return "s3://%s/%s".formatted(bucketName, path);
    }
}
