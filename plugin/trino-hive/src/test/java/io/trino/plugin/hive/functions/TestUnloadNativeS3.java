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

import java.io.IOException;
import java.nio.file.Files;

public class TestUnloadNativeS3
        extends BaseUnloadFileSystemTest
{
    private static final String bucketName = requireEnv("S3_BUCKET");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("fs.hadoop.enabled", "false")
                        .put("fs.native-s3.enabled", "true")
                        .put("s3.region", requireEnv("AWS_REGION"))
                        .put("s3.aws-access-key", requireEnv("AWS_ACCESS_KEY_ID"))
                        .put("s3.aws-secret-key", requireEnv("AWS_SECRET_ACCESS_KEY"))
                        .put("hive.metastore", "file")
                        .put("hive.metastore.catalog.dir", "local://" + Files.createTempDirectory("metastore"))
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
                        .setRegion(requireEnv("AWS_REGION"))
                        .setAwsAccessKey(requireEnv("AWS_ACCESS_KEY_ID"))
                        .setAwsSecretKey(requireEnv("AWS_SECRET_ACCESS_KEY")));
    }

    @Override
    protected String getLocation(String path)
    {
        return "s3://%s/%s".formatted(bucketName, path);
    }
}
