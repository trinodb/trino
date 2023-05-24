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

import com.google.common.collect.ImmutableSet;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3ConfigurationInitializer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.testng.Assert.assertFalse;
import static org.testng.util.Strings.isNullOrEmpty;

public abstract class AbstractTestHiveFileSystemS3
        extends AbstractTestHiveFileSystem
{
    private String awsAccessKey;
    private String awsSecretKey;
    private String writableBucket;
    private String testDirectory;

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
}
