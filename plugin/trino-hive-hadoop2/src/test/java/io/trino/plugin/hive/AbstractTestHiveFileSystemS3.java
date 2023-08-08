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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.common.collect.ImmutableList;
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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static java.io.InputStream.nullInputStream;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.util.Strings.isNullOrEmpty;

public abstract class AbstractTestHiveFileSystemS3
        extends AbstractTestHiveFileSystem
{
    private static final MediaType DIRECTORY_MEDIA_TYPE = MediaType.create("application", "x-directory");

    private String awsAccessKey;
    private String awsSecretKey;
    private String writableBucket;
    private String testDirectory;
    private AmazonS3 s3Client;

    protected void setup(
            String host,
            int port,
            String databaseName,
            String s3endpoint,
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
        checkArgument(!isNullOrEmpty(s3endpoint), "Expected non empty s3endpoint");
        checkArgument(!isNullOrEmpty(writableBucket), "Expected non empty writableBucket");
        checkArgument(!isNullOrEmpty(testDirectory), "Expected non empty testDirectory");
        this.awsAccessKey = awsAccessKey;
        this.awsSecretKey = awsSecretKey;
        this.writableBucket = writableBucket;
        this.testDirectory = testDirectory;

        s3Client = AmazonS3Client.builder()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(s3endpoint, null))
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsAccessKey, awsSecretKey)))
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

    /**
     * Tests the same functionality like {@link #testFileIteratorPartitionedListing()} with the
     * setup done by native {@link AmazonS3}
     */
    @Test
    public void testFileIteratorPartitionedListingNativeS3Client()
            throws Exception
    {
        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(table.getSchemaName())
                .setTableName(table.getTableName())
                .setDataColumns(ImmutableList.of(new Column("data", HIVE_LONG, Optional.empty())))
                .setPartitionColumns(ImmutableList.of(new Column("part", HIVE_STRING, Optional.empty())))
                .setOwner(Optional.empty())
                .setTableType("fake");
        tableBuilder.getStorageBuilder()
                .setStorageFormat(StorageFormat.fromHiveStorageFormat(HiveStorageFormat.CSV));
        Table fakeTable = tableBuilder.build();

        Path basePath = new Path(getBasePath(), "test-file-iterator-partitioned-listing-native-setup");
        FileSystem fs = hdfsEnvironment.getFileSystem(TESTING_CONTEXT, basePath);
        TrinoFileSystem trinoFileSystem = new HdfsFileSystemFactory(hdfsEnvironment, new TrinoHdfsFileSystemStats()).create(SESSION);
        fs.mkdirs(basePath);
        String basePrefix = basePath.toUri().getPath().substring(1);

        // Expected file system tree:
        // test-file-iterator-partitioned-listing-native-setup/
        //      .hidden/
        //          nested-file-in-hidden.txt
        //      part=simple/
        //          _hidden-file.txt
        //          plain-file.txt
        //      part=nested/
        //          parent/
        //             _nested-hidden-file.txt
        //             nested-file.txt
        //      part=plus+sign/
        //          plus-file.txt
        //      part=percent%sign/
        //          percent-file.txt
        //      part=url%20encoded/
        //          url-encoded-file.txt
        //      part=level1|level2/
        //          pipe-file.txt
        //          parent1/
        //             parent2/
        //                deeply-nested-file.txt
        //      part=level1 | level2/
        //          pipe-blanks-file.txt
        //      empty-directory/
        //      .hidden-in-base.txt

        createFile(writableBucket, format("%s/.hidden/nested-file-in-hidden.txt", basePrefix));
        createFile(writableBucket, format("%s/part=simple/_hidden-file.txt", basePrefix));
        createFile(writableBucket, format("%s/part=simple/plain-file.txt", basePrefix));
        createFile(writableBucket, format("%s/part=nested/parent/_nested-hidden-file.txt", basePrefix));
        createFile(writableBucket, format("%s/part=nested/parent/nested-file.txt", basePrefix));
        createFile(writableBucket, format("%s/part=plus+sign/plus-file.txt", basePrefix));
        createFile(writableBucket, format("%s/part=percent%%sign/percent-file.txt", basePrefix));
        createFile(writableBucket, format("%s/part=url%%20encoded/url-encoded-file.txt", basePrefix));
        createFile(writableBucket, format("%s/part=level1|level2/pipe-file.txt", basePrefix));
        createFile(writableBucket, format("%s/part=level1|level2/parent1/parent2/deeply-nested-file.txt", basePrefix));
        createFile(writableBucket, format("%s/part=level1 | level2/pipe-blanks-file.txt", basePrefix));
        createDirectory(writableBucket, format("%s/empty-directory/", basePrefix));
        createFile(writableBucket, format("%s/.hidden-in-base.txt", basePrefix));

        // List recursively through hive file iterator
        HiveFileIterator recursiveIterator = new HiveFileIterator(
                fakeTable,
                Location.of(basePath.toString()),
                trinoFileSystem,
                new FileSystemDirectoryLister(),
                new HdfsNamenodeStats(),
                HiveFileIterator.NestedDirectoryPolicy.RECURSE);

        List<String> recursiveListing = Streams.stream(recursiveIterator)
                .map(TrinoFileStatus::getPath)
                .toList();
        // Should not include directories, or files underneath hidden directories
        assertThat(recursiveListing).containsExactlyInAnyOrder(
                format("%s/part=simple/plain-file.txt", basePath),
                format("%s/part=nested/parent/nested-file.txt", basePath),
                format("%s/part=plus+sign/plus-file.txt", basePath),
                format("%s/part=percent%%sign/percent-file.txt", basePath),
                format("%s/part=url%%20encoded/url-encoded-file.txt", basePath),
                format("%s/part=level1|level2/pipe-file.txt", basePath),
                format("%s/part=level1|level2/parent1/parent2/deeply-nested-file.txt", basePath),
                format("%s/part=level1 | level2/pipe-blanks-file.txt", basePath));

        HiveFileIterator shallowIterator = new HiveFileIterator(
                fakeTable,
                Location.of(basePath.toString()),
                trinoFileSystem,
                new FileSystemDirectoryLister(),
                new HdfsNamenodeStats(),
                HiveFileIterator.NestedDirectoryPolicy.IGNORED);
        List<Path> shallowListing = Streams.stream(shallowIterator)
                .map(TrinoFileStatus::getPath)
                .map(Path::new)
                .toList();
        // Should not include any hidden files, folders, or nested files
        assertThat(shallowListing).isEmpty();
    }

    protected void createDirectory(String bucketName, String key)
    {
        // create meta-data for your folder and set content-length to 0
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        metadata.setContentType(DIRECTORY_MEDIA_TYPE.toString());
        // create a PutObjectRequest passing the folder name suffixed by /
        if (!key.endsWith("/")) {
            key += "/";
        }
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, nullInputStream(), metadata);
        // send request to S3 to create folder
        s3Client.putObject(putObjectRequest);
    }

    protected void createFile(String bucketName, String key)
    {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, nullInputStream(), metadata);
        s3Client.putObject(putObjectRequest);
    }
}
