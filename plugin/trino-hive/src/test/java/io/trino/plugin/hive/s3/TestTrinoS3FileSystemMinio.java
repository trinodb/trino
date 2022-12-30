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
package io.trino.plugin.hive.s3;

import com.amazonaws.services.s3.AmazonS3;
import io.trino.testing.containers.Minio;
import io.trino.testing.minio.MinioClient;
import io.trino.util.AutoCloseableCloser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;

import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_ACCESS_KEY;
import static io.trino.testing.containers.Minio.MINIO_SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

public class TestTrinoS3FileSystemMinio
        extends BaseTestTrinoS3FileSystemObjectStorage
{
    private final String bucketName = "trino-ci-test";

    private Minio minio;

    private MinioClient minioClient;

    @BeforeClass
    public void setup()
            throws Exception
    {
        minio = Minio.builder().build();
        minio.start();

        minioClient = minio.createMinioClient();
        minio.createBucket(bucketName);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        try (AutoCloseableCloser closer = AutoCloseableCloser.create()) {
            closer.register(minio);
            closer.register(minioClient);
        }
        minioClient = null;
        minio = null;
    }

    @Override
    protected String getBucketName()
    {
        return bucketName;
    }

    @Override
    protected Configuration s3Configuration()
    {
        Configuration config = newEmptyConfiguration();
        config.set("trino.s3.endpoint", minio.getMinioAddress());
        config.set("trino.s3.access-key", MINIO_ACCESS_KEY);
        config.set("trino.s3.secret-key", MINIO_SECRET_KEY);
        config.set("trino.s3.path-style-access", "true");

        return config;
    }

    @Test
    public void testDeleteNonRecursivelyEmptyBucketRoot()
            throws Exception
    {
        String testBucketName = "trino-delete-bucket-root-empty" + randomNameSuffix();
        minioClient.makeBucket(testBucketName);
        String testBucketPath = "s3://%s/".formatted(testBucketName);
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI(testBucketPath), s3Configuration());

            AmazonS3 s3 = fs.getS3Client();

            assertThat(listPaths(s3, testBucketName, "", true)).isEmpty();

            fs.delete(new Path(testBucketPath), false);

            assertThat(listPaths(s3, testBucketName, "", true)).isEmpty();
        }
    }

    @Test
    public void testDeleteNonRecursivelyNonEmptyBucketRoot()
            throws Exception
    {
        String testBucketName = "trino-delete-bucket-root-non-empty" + randomNameSuffix();
        minioClient.makeBucket(testBucketName);
        String testBucketPath = "s3://%s/".formatted(testBucketName);
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI(testBucketPath), s3Configuration());

            AmazonS3 s3 = fs.getS3Client();
            fs.createNewFile(new Path("s3://%s/file1.txt".formatted(testBucketName)));
            String directory2Path = testBucketPath + "directory2";
            createDirectory(fs.getS3Client(), testBucketName, "directory2");
            String filename2 = "file2.txt";
            fs.createNewFile(new Path(directory2Path, filename2));

            assertThat(listPaths(s3, testBucketName, "", true))
                    .containsOnly("file1.txt", "directory2/", "directory2/file2.txt");

            assertThatThrownBy(() -> fs.delete(new Path(testBucketPath), false))
                    .hasMessage("Directory %s is not empty".formatted(testBucketPath));

            assertThat(listPaths(s3, testBucketName, "", true))
                    .containsOnly("file1.txt", "directory2/", "directory2/file2.txt");
        }
    }

    @Test
    public void testDeleteRecursivelyBucketRoot()
            throws Exception
    {
        String testBucketName = "trino-delete-recursive-bucket-root" + randomNameSuffix();
        minioClient.makeBucket(testBucketName);
        String testBucketPath = "s3://" + testBucketName;
        try (TrinoS3FileSystem fs = new TrinoS3FileSystem()) {
            fs.initialize(new URI(testBucketPath), s3Configuration());

            AmazonS3 s3 = fs.getS3Client();
            fs.createNewFile(new Path("s3://%s/file1.txt".formatted(testBucketName)));
            String directory2Path = testBucketPath + "/directory2";
            createDirectory(fs.getS3Client(), testBucketName, "directory2");
            fs.createNewFile(new Path(directory2Path, "file2.txt"));

            assertThat(listPaths(s3, testBucketName, "", true))
                    .containsOnly("file1.txt", "directory2/", "directory2/file2.txt");

            assertTrue(fs.delete(new Path(testBucketPath + Path.SEPARATOR), true));

            assertThat(listPaths(s3, testBucketName, "", true)).isEmpty();
        }
    }
}
