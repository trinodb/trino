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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.net.MediaType;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsNamenodeStats;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3ConfigurationInitializer;
import io.trino.plugin.hive.fs.FileSystemDirectoryLister;
import io.trino.plugin.hive.fs.HiveFileIterator;
import io.trino.plugin.hive.fs.TrinoFileStatus;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static java.lang.String.format;
import static org.testng.Assert.assertFalse;
import static org.testng.util.Strings.isNullOrEmpty;

public abstract class AbstractTestHiveFileSystemS3
        extends AbstractTestHiveFileSystem
{
    private static final MediaType DIRECTORY_MEDIA_TYPE = MediaType.create("application", "x-directory");
    private static final String PATH_SEPARATOR = "/";

    private String awsAccessKey;
    private String awsSecretKey;
    private String writableBucket;
    private String testDirectory;
    private S3Client s3Client;

    protected void setup(
            String host,
            int port,
            String databaseName,
            String awsAccessKey,
            String awsSecretKey,
            String writableBucket,
            String testDirectory,
            boolean s3SelectPushdownEnabled)
    {
        checkArgument(!isNullOrEmpty(host), "Expected non empty host");
        checkArgument(!isNullOrEmpty(databaseName), "Expected non empty databaseName");
        checkArgument(!isNullOrEmpty(awsAccessKey), "Expected non empty awsAccessKey");
        checkArgument(!isNullOrEmpty(awsSecretKey), "Expected non empty awsSecretKey");
        checkArgument(!isNullOrEmpty(writableBucket), "Expected non empty writableBucket");
        checkArgument(!isNullOrEmpty(testDirectory), "Expected non empty testDirectory");
        this.awsAccessKey = awsAccessKey;
        this.awsSecretKey = awsSecretKey;
        this.writableBucket = writableBucket;
        this.testDirectory = testDirectory;
        this.s3Client = S3Client.builder()
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(awsAccessKey, awsSecretKey)))
                .build();
        setup(host, port, databaseName, s3SelectPushdownEnabled, createHdfsConfiguration());
    }

    private HdfsConfiguration createHdfsConfiguration()
    {
        ConfigurationInitializer s3Config = new TrinoS3ConfigurationInitializer(new HiveS3Config()
                .setS3AwsAccessKey(awsAccessKey)
                .setS3AwsSecretKey(awsSecretKey));
        HdfsConfigurationInitializer initializer = new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of(s3Config));
        return new DynamicHdfsConfiguration(initializer, ImmutableSet.of());
    }

    @Override
    protected Path getBasePath()
    {
        // HDP 3.1 does not understand s3:// out of the box.
        return new Path(format("s3a://%s/%s/", writableBucket, testDirectory));
    }

    @Test
    public void testIgnoreHadoopFolderMarker()
            throws Exception
    {
        Path basePath = getBasePath();
        FileSystem fs = hdfsEnvironment.getFileSystem(TESTING_CONTEXT, basePath);

        String markerFileName = "test_table_$folder$";
        Path filePath = new Path(basePath, markerFileName);
        fs.create(filePath).close();

        assertFalse(Arrays.stream(fs.listStatus(basePath)).anyMatch(file -> file.getPath().getName().equalsIgnoreCase(markerFileName)));
    }

    @Test
    public void testFileIteratorTooSlashedListing()
            throws Exception
    {
        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(table.getSchemaName())
                .setTableName(table.getTableName())
                .setDataColumns(ImmutableList.of(new Column("data", HIVE_LONG, Optional.empty())))
                .setPartitionColumns(ImmutableList.of())
                .setOwner(Optional.empty())
                .setTableType("fake");
        tableBuilder.getStorageBuilder()
                .setStorageFormat(StorageFormat.fromHiveStorageFormat(HiveStorageFormat.CSV));
        Table fakeTable = tableBuilder.build();

        // Expected file system tree:
        // test-file-iterator-too-slashed-listing/
        //      .hidden/
        //          nested-file-in-hidden.txt
        //      nested/
        //          _nested-hidden-file.txt
        //          nested-file.txt
        //      /
        //         too-slashed-nested-file.txt
        //      base-path-file.txt
        //      empty-directory/
        //      .hidden-in-base.txt
        Path basePath = new Path(getBasePath(), "test-file-iterator-too-slashed-listing");
        FileSystem fs = hdfsEnvironment.getFileSystem(TESTING_CONTEXT, basePath);
        TrinoFileSystem trinoFileSystem = new HdfsFileSystemFactory(hdfsEnvironment, new TrinoHdfsFileSystemStats()).create(SESSION);
        fs.mkdirs(basePath);
        String basePrefix = basePath.toUri().getPath().substring(1);

        // create file in hidden folder
        createFile(writableBucket, basePrefix, ".hidden/nested-file-in-hidden.txt");
        // create nested file in non-hidden folder
        createFile(writableBucket, basePrefix, "nested/nested-file.txt");
        // create hidden file in non-hidden folder
        createFile(writableBucket, basePrefix, "nested/_nested-hidden-file.txt");
        createFile(writableBucket, basePrefix, "/too-slashed-nested-file.txt");
        // create file in base path
        createFile(writableBucket, basePrefix, "base-path-file.txt");
        // create hidden file in base path
        createFile(writableBucket, basePrefix, ".hidden-in-base.txt");
        // create empty subdirectory
        createDirectory(writableBucket, basePrefix, "empty-directory");

        // List recursively through hive file iterator
        HiveFileIterator recursiveIterator = new HiveFileIterator(
                fakeTable,
                Location.of(basePath.toString()),
                trinoFileSystem,
                new FileSystemDirectoryLister(),
                new HdfsNamenodeStats(),
                HiveFileIterator.NestedDirectoryPolicy.RECURSE);

        List<String> recursiveDirectoryListing = Streams.stream(recursiveIterator)
                .map(TrinoFileStatus::getPath)
                .map(s -> s.substring(basePath.toString().length() + 1))
                .toList();
        // Should not include directories, or files underneath hidden directories
        assertEqualsIgnoreOrder(
                recursiveDirectoryListing,
                ImmutableList.of(
                        //TODO FIXME the key suffix `too-slashed-nested-file.txt` should be actually `/too-slashed-nested-file.txt`
                        "too-slashed-nested-file.txt",
                        "base-path-file.txt",
                        "nested/nested-file.txt"));

        HiveFileIterator shallowIterator = new HiveFileIterator(
                fakeTable,
                Location.of(basePath.toString()),
                trinoFileSystem,
                new FileSystemDirectoryLister(),
                new HdfsNamenodeStats(),
                HiveFileIterator.NestedDirectoryPolicy.IGNORED);
        List<String> shallowDirectoryListing = Streams.stream(shallowIterator)
                .map(TrinoFileStatus::getPath)
                .map(s -> s.substring(basePath.toString().length() + 1))
                .toList();
        assertEqualsIgnoreOrder(shallowDirectoryListing, ImmutableList.of(
                //TODO FIXME the file `too-slashed-nested-file.txt` should not be retrieved because it is not at the base level in the directory
                "too-slashed-nested-file.txt",
                "base-path-file.txt"));
    }

    private void createDirectory(String bucketName, String prefix, String key)
    {
        // create a PutObjectRequest passing the folder name suffixed by /
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(format("%s/%s", prefix, key) + PATH_SEPARATOR)
                // create meta-data for your folder and set content-length to 0
                .metadata(ImmutableMap.of(
                        "Content-Length", "0",
                        "Content-Type", DIRECTORY_MEDIA_TYPE.toString()))
                .build();
        // send request to S3 to create folder
        s3Client.putObject(putObjectRequest, RequestBody.empty());
    }

    private void createFile(String bucketName, String prefix, String key)
    {
        s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(format("%s/%s", prefix, key))
                        .build(),
                RequestBody.empty());
    }
}
