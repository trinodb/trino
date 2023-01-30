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
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.net.MediaType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertTrue;

public abstract class BaseTestTrinoS3FileSystemObjectStorage
{
    private static final MediaType DIRECTORY_MEDIA_TYPE = MediaType.create("application", "x-directory");
    private static final String PATH_SEPARATOR = "/";
    private static final String DIRECTORY_SUFFIX = "_$folder$";

    protected abstract String getBucketName();

    protected abstract Configuration s3Configuration();

    @Test
    public void testDeleteRecursivelyMissingObjectPath()
            throws Exception
    {
        String prefix = "test-delete-recursively-missing-object-" + randomNameSuffix();

        try (TrinoS3FileSystem fs = createFileSystem()) {
            // Follow Amazon S3 behavior if attempting to delete an object that does not exist
            // and return a success message
            assertTrue(fs.delete(new Path("s3://%s/%s".formatted(getBucketName(), prefix)), true));
        }
    }

    @Test
    public void testDeleteNonRecursivelyMissingObjectPath()
            throws Exception
    {
        String prefix = "test-delete-non-recursively-missing-object-" + randomNameSuffix();

        try (TrinoS3FileSystem fs = createFileSystem()) {
            // Follow Amazon S3 behavior if attempting to delete an object that does not exist
            // and return a success message
            assertTrue(fs.delete(new Path("s3://%s/%s".formatted(getBucketName(), prefix)), false));
        }
    }

    @Test
    public void testDeleteRecursivelyObjectPath()
            throws Exception
    {
        String prefix = "test-delete-recursively-object-" + randomNameSuffix();
        String prefixPath = "s3://%s/%s".formatted(getBucketName(), prefix);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                String filename = "file.txt";
                String fileKey = "%s/%s".formatted(prefix, filename);
                String filePath = "s3://%s/%s/%s".formatted(getBucketName(), prefix, filename);
                fs.createNewFile(new Path(prefixPath, filename));
                List<String> paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(fileKey);

                assertTrue(fs.delete(new Path(filePath), true));

                assertThat(listPaths(fs.getS3Client(), getBucketName(), prefix, true)).isEmpty();
            }
            finally {
                fs.delete(new Path(prefixPath), true);
            }
        }
    }

    @Test
    public void testDeleteNonRecursivelyObjectPath()
            throws Exception
    {
        String prefix = "test-delete-non-recursively-object-" + randomNameSuffix();
        String prefixPath = "s3://%s/%s".formatted(getBucketName(), prefix);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                String filename = "file.txt";
                String fileKey = "%s/%s".formatted(prefix, filename);
                String filePath = "s3://%s/%s".formatted(getBucketName(), fileKey);
                fs.createNewFile(new Path(prefixPath, filename));
                List<String> paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(fileKey);

                assertTrue(fs.delete(new Path(filePath), false));

                assertThat(listPaths(fs.getS3Client(), getBucketName(), prefix, true)).isEmpty();
            }
            finally {
                fs.delete(new Path(prefixPath), true);
            }
        }
    }

    @Test
    public void testDeleteNonRecursivelyObjectNamePrefixingAnotherObjectName()
            throws Exception
    {
        String prefix = "test-delete-non-recursively-object-delete-only-requested-object-" + randomNameSuffix();
        String prefixPath = "s3://%s/%s".formatted(getBucketName(), prefix);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                fs.createNewFile(new Path(prefixPath, "foo"));
                fs.createNewFile(new Path(prefixPath, "foobar"));
                List<String> paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(
                        "%s/foo".formatted(prefix),
                        "%s/foobar".formatted(prefix));

                assertTrue(fs.delete(new Path("s3://%s/%s/foo".formatted(getBucketName(), prefix)), false));

                paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(
                        "%s/foobar".formatted(prefix));
            }
            finally {
                fs.delete(new Path(prefixPath), true);
            }
        }
    }

    @Test
    public void testDeleteNonRecursivelyDirectoryNamePrefixingAnotherDirectoryName()
            throws Exception
    {
        String prefix = "test-delete-non-recursively-object-delete-only-requested-directory-" + randomNameSuffix();
        String prefixPath = "s3://%s/%s".formatted(getBucketName(), prefix);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                createDirectory(fs.getS3Client(), getBucketName(), "%s/foo".formatted(prefix));
                createDirectory(fs.getS3Client(), getBucketName(), "%s/foobar".formatted(prefix));
                List<String> paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(
                        "%s/foo/".formatted(prefix),
                        "%s/foobar/".formatted(prefix));

                assertTrue(fs.delete(new Path("s3://%s/%s/foo".formatted(getBucketName(), prefix)), true));

                paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(
                        "%s/foobar/".formatted(prefix));
            }
            finally {
                fs.delete(new Path(prefixPath), true);
            }
        }
    }

    @Test
    public void testDeleteNonRecursivelyEmptyDirectory()
            throws Exception
    {
        String prefix = "test-delete-non-recursively-empty-directory-" + randomNameSuffix();
        String prefixPath = "s3://%s/%s".formatted(getBucketName(), prefix);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                createDirectory(fs.getS3Client(), getBucketName(), prefix);
                List<String> paths = listPaths(fs.getS3Client(), getBucketName(), prefix, false);
                assertThat(paths).containsOnly(prefix + PATH_SEPARATOR);

                assertTrue(fs.delete(new Path(prefixPath), false));

                assertThat(listPaths(fs.getS3Client(), getBucketName(), prefix, true)).isEmpty();
            }
            finally {
                fs.delete(new Path(prefixPath), true);
            }
        }
    }

    @Test
    public void testDeleteNonRecursivelyEmptyDirectoryWithAdditionalDirectorySuffixPlaceholder()
            throws Exception
    {
        String directoryName = "test-delete-non-recursively-empty-directory-" + randomNameSuffix();
        String directoryPath = "s3://%s/%s".formatted(getBucketName(), directoryName);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                createDirectory(fs.getS3Client(), getBucketName(), directoryName);
                fs.createNewFile(new Path(directoryPath + DIRECTORY_SUFFIX));
                List<String> paths = listPaths(fs.getS3Client(), getBucketName(), directoryName, true);
                assertThat(paths).containsOnly(
                        directoryName + PATH_SEPARATOR,
                        directoryName + DIRECTORY_SUFFIX);

                assertTrue(fs.delete(new Path(directoryPath), false));

                assertThat(listPaths(fs.getS3Client(), getBucketName(), directoryName, true)).isEmpty();
            }
            finally {
                fs.delete(new Path(directoryPath), true);
            }
        }
    }

    @Test
    public void testDeleteRecursivelyObjectNamePrefixingAnotherObjectName()
            throws Exception
    {
        String prefix = "test-delete-recursively-object-delete-only-requested-object-" + randomNameSuffix();
        String prefixPath = "s3://%s/%s".formatted(getBucketName(), prefix);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                fs.createNewFile(new Path(prefixPath, "foo"));
                fs.createNewFile(new Path(prefixPath, "foobar"));
                List<String> paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(
                        "%s/foo".formatted(prefix),
                        "%s/foobar".formatted(prefix));

                assertTrue(fs.delete(new Path("s3://%s/%s/foo".formatted(getBucketName(), prefix)), true));

                paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(
                        "%s/foobar".formatted(prefix));
            }
            finally {
                fs.delete(new Path(prefixPath), true);
            }
        }
    }

    @Test
    public void testDeleteRecursivelyDirectoryNamePrefixingAnotherDirectoryName()
            throws Exception
    {
        String prefix = "test-delete-recursively-object-delete-only-requested-directory-" + randomNameSuffix();
        String prefixPath = "s3://%s/%s".formatted(getBucketName(), prefix);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                createDirectory(fs.getS3Client(), getBucketName(), "%s/foo".formatted(prefix));
                createDirectory(fs.getS3Client(), getBucketName(), "%s/foobar".formatted(prefix));
                List<String> paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(
                        "%s/foo/".formatted(prefix),
                        "%s/foobar/".formatted(prefix));

                assertTrue(fs.delete(new Path("s3://%s/%s/foo".formatted(getBucketName(), prefix)), true));

                paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly("%s/foobar/".formatted(prefix));
            }
            finally {
                fs.delete(new Path(prefixPath), true);
            }
        }
    }

    @Test
    public void testDeleteRecursivelyPrefixContainingMultipleObjectsPlain()
            throws Exception
    {
        String prefix = "test-delete-recursively-path-multiple-objects-plain-" + randomNameSuffix();
        String prefixPath = "s3://%s/%s".formatted(getBucketName(), prefix);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                String filename1 = "file1.txt";
                String filename2 = "file2.txt";
                fs.createNewFile(new Path(prefixPath, filename1));
                fs.createNewFile(new Path(prefixPath, filename2));
                List<String> paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(
                        "%s/%s".formatted(prefix, filename1),
                        "%s/%s".formatted(prefix, filename2));

                assertTrue(fs.delete(new Path(prefixPath), true));

                assertThat(listPaths(fs.getS3Client(), getBucketName(), prefix, true)).isEmpty();
            }
            finally {
                fs.delete(new Path(prefixPath), true);
            }
        }
    }

    @Test
    public void testDeleteRecursivelyDirectoryWithDeepHierarchy()
            throws Exception
    {
        String prefix = "test-delete-recursively-directory-deep-hierarchy-" + randomNameSuffix();
        String prefixPath = "s3://%s/%s".formatted(getBucketName(), prefix);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                String directoryKey = prefix + "/directory";
                String directoryPath = "s3://%s/%s".formatted(getBucketName(), directoryKey);
                createDirectory(fs.getS3Client(), getBucketName(), directoryKey);

                String filename1 = "file1.txt";
                String filename2 = "file2.txt";
                String filename3 = "file3.txt";
                fs.createNewFile(new Path(directoryPath, filename1));
                fs.createNewFile(new Path(directoryPath, filename2));
                fs.createNewFile(new Path(directoryPath + "/dir3", filename3));
                createDirectory(fs.getS3Client(), getBucketName(), directoryKey + "/dir4");

                assertTrue(fs.delete(new Path(directoryPath), true));

                assertThat(listPaths(fs.getS3Client(), getBucketName(), prefix, true)).isEmpty();
            }
            finally {
                fs.delete(new Path(prefixPath), true);
            }
        }
    }

    @Test
    public void testDeleteRecursivelyEmptyDirectory()
            throws Exception
    {
        String prefix = "test-delete-recursively-empty-directory-" + randomNameSuffix();
        String prefixPath = "s3://%s/%s".formatted(getBucketName(), prefix);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                String directoryKey = prefix + "/directory";
                createDirectory(fs.getS3Client(), getBucketName(), directoryKey);
                fs.createNewFile(new Path("s3://%s/%s%s".formatted(getBucketName(), directoryKey, DIRECTORY_SUFFIX)));
                List<String> paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(
                        directoryKey + PATH_SEPARATOR,
                        directoryKey + DIRECTORY_SUFFIX);

                assertTrue(fs.delete(new Path(prefixPath + "/directory"), true));

                assertThat(listPaths(fs.getS3Client(), getBucketName(), prefix, true)).isEmpty();
            }
            finally {
                fs.delete(new Path(prefixPath), true);
            }
        }
    }

    @Test
    public void testDeleteRecursivelyDirectoryWithObjectsAndDirectorySuffixPlaceholder()
            throws Exception
    {
        String prefix = "test-delete-recursively-directory-multiple-objects-" + randomNameSuffix();
        String prefixPath = "s3://%s/%s".formatted(getBucketName(), prefix);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                String directoryKey = prefix + "/directory";
                String directoryPath = "s3://%s/%s".formatted(getBucketName(), directoryKey);
                createDirectory(fs.getS3Client(), getBucketName(), directoryKey);
                fs.createNewFile(new Path(directoryPath + DIRECTORY_SUFFIX));

                String filename1 = "file1.txt";
                String filename2 = "file2.txt";
                String filename3 = "file3.txt";
                fs.createNewFile(new Path(directoryPath, filename1));
                fs.createNewFile(new Path(directoryPath, filename2));
                fs.createNewFile(new Path(directoryPath + "/dir3", filename3));
                fs.createNewFile(new Path(directoryPath + "/dir3" + DIRECTORY_SUFFIX));
                createDirectory(fs.getS3Client(), getBucketName(), directoryKey + "/dir4");
                fs.createNewFile(new Path(directoryPath + "/dir4" + DIRECTORY_SUFFIX));

                assertTrue(fs.delete(new Path(directoryPath), true));

                assertThat(listPaths(fs.getS3Client(), getBucketName(), prefix, true)).isEmpty();
            }
            finally {
                fs.delete(new Path(prefixPath), true);
            }
        }
    }

    @Test
    public void testDeleteRecursivelyPrefixContainingDeepHierarchy()
            throws Exception
    {
        String prefix = "test-delete-recursively-prefix-deep-hierarchy-" + randomNameSuffix();
        String prefixPath = "s3://%s/%s".formatted(getBucketName(), prefix);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                String filename1 = "file1.txt";
                String filename2 = "file2.txt";
                String filename3 = "file3.txt";
                fs.createNewFile(new Path("s3://%s/%s/dir1".formatted(getBucketName(), prefix), filename1));
                fs.createNewFile(new Path("s3://%s/%s/dir2/dir22".formatted(getBucketName(), prefix), filename2));
                fs.createNewFile(new Path("s3://%s/%s/dir3/dir33/dir333".formatted(getBucketName(), prefix), filename3));
                List<String> paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(
                        "%s/dir1/%s".formatted(prefix, filename1),
                        "%s/dir2/dir22/%s".formatted(prefix, filename2),
                        "%s/dir3/dir33/dir333/%s".formatted(prefix, filename3));

                assertTrue(fs.delete(new Path(prefixPath), true));

                assertThat(listPaths(fs.getS3Client(), getBucketName(), prefix, true)).isEmpty();
            }
            finally {
                fs.delete(new Path(prefixPath), true);
            }
        }
    }

    @Test
    public void testDeleteNonRecursivelyNonEmptyDirectory()
            throws Exception
    {
        String prefix = "test-illegal-delete-non-recursively-directory-non-empty-" + randomNameSuffix();
        String prefixPath = "s3://%s/%s".formatted(getBucketName(), prefix);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                String directoryKey = prefix + "/directory";
                String directoryPath = "s3://%s/%s".formatted(getBucketName(), directoryKey);
                createDirectory(fs.getS3Client(), getBucketName(), directoryKey);

                fs.createNewFile(new Path(directoryPath, "file1.txt"));

                assertThatThrownBy(() -> fs.delete(new Path(directoryPath), false))
                        .hasMessage("Directory %s is not empty".formatted(directoryPath));

                List<String> paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(
                        "%s/directory/".formatted(prefix),
                        "%s/directory/file1.txt".formatted(prefix));
            }
            finally {
                fs.delete(new Path(prefixPath), true);
            }
        }
    }

    @Test
    public void testDeleteNonRecursivelyNonEmptyPath()
            throws Exception
    {
        String prefix = "test-illegal-delete-non-recursively-path-non-empty-" + randomNameSuffix();
        String prefixPath = "s3://%s/%s".formatted(getBucketName(), prefix);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                fs.createNewFile(new Path(prefixPath, "file1.txt"));

                assertThatThrownBy(() -> fs.delete(new Path("s3://%s/%s".formatted(getBucketName(), prefix)), false))
                        .hasMessage("Directory s3://%s/%s is not empty".formatted(getBucketName(), prefix));

                List<String> paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(
                        "%s/file1.txt".formatted(prefix));
            }
            finally {
                fs.delete(new Path(prefixPath), true);
            }
        }
    }

    @Test
    public void testDeleteNonRecursivelyNonEmptyDeepPath()
            throws Exception
    {
        String prefix = "test-illegal-delete-non-recursively-deep-path-non-empty-" + randomNameSuffix();
        String prefixPath = "s3://%s/%s".formatted(getBucketName(), prefix);

        try (TrinoS3FileSystem fs = createFileSystem()) {
            try {
                String filename1 = "file1.txt";
                String filename2 = "file2.txt";
                fs.createNewFile(new Path(prefixPath + "/dir1/", filename1));
                fs.createNewFile(new Path(prefixPath + "/dir2/", filename2));
                List<String> paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(
                        "%s/dir1/%s".formatted(prefix, filename1),
                        "%s/dir2/%s".formatted(prefix, filename2));

                assertThatThrownBy(() -> fs.delete(new Path("s3://%s/%s".formatted(getBucketName(), prefix)), false))
                        .hasMessage("Directory s3://%s/%s is not empty".formatted(getBucketName(), prefix));

                paths = listPaths(fs.getS3Client(), getBucketName(), prefix, true);
                assertThat(paths).containsOnly(
                        "%s/dir1/%s".formatted(prefix, filename1),
                        "%s/dir2/%s".formatted(prefix, filename2));
            }
            finally {
                fs.delete(new Path(prefixPath), true);
            }
        }
    }

    protected TrinoS3FileSystem createFileSystem()
            throws Exception
    {
        TrinoS3FileSystem fs = new TrinoS3FileSystem();
        try {
            fs.initialize(new URI("s3://%s/".formatted(getBucketName())), s3Configuration());
        }
        catch (Throwable e) {
            closeAllSuppress(e, fs);
            throw e;
        }
        return fs;
    }

    protected static void createDirectory(AmazonS3 client, String bucketName, String key)
    {
        // create meta-data for your folder and set content-length to 0
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        metadata.setContentType(DIRECTORY_MEDIA_TYPE.toString());
        // create empty content
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
        // create a PutObjectRequest passing the folder name suffixed by /
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key + PATH_SEPARATOR, emptyContent, metadata);
        // send request to S3 to create folder
        client.putObject(putObjectRequest);
    }

    protected static List<String> listPaths(AmazonS3 s3, String bucketName, String prefix, boolean recursive)
    {
        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(bucketName)
                .withPrefix(prefix)
                .withDelimiter(recursive ? null : PATH_SEPARATOR);
        ListObjectsV2Result listing = s3.listObjectsV2(request);

        List<String> paths = new ArrayList<>();
        paths.addAll(listing.getCommonPrefixes());
        paths.addAll(listing.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(toImmutableList()));
        return paths;
    }
}
