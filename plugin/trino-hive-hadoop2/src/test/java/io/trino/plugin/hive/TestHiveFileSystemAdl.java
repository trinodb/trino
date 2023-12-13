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
import io.trino.hdfs.azure.HiveAzureConfig;
import io.trino.hdfs.azure.TrinoAzureConfigurationInitializer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.FileNotFoundException;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.util.Strings.isNullOrEmpty;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestHiveFileSystemAdl
        extends AbstractTestHiveFileSystem
{
    private String dataLakeName;
    private String clientId;
    private String credential;
    private String refreshUrl;
    private String testDirectory;

    @BeforeAll
    public void setup()
    {
        String host = System.getProperty("hive.hadoop2.metastoreHost");
        int port = Integer.getInteger("hive.hadoop2.metastorePort");
        String databaseName = System.getProperty("hive.hadoop2.databaseName");
        String dataLakeName = System.getProperty("hive.hadoop2.adl.name");
        String clientId = System.getProperty("hive.hadoop2.adl.clientId");
        String credential = System.getProperty("hive.hadoop2.adl.credential");
        String refreshUrl = System.getProperty("hive.hadoop2.adl.refreshUrl");
        String testDirectory = System.getProperty("hive.hadoop2.adl.testDirectory");

        checkArgument(!isNullOrEmpty(host), "expected non empty host");
        checkArgument(!isNullOrEmpty(databaseName), "expected non empty databaseName");
        checkArgument(!isNullOrEmpty(dataLakeName), "expected non empty dataLakeName");
        checkArgument(!isNullOrEmpty(clientId), "expected non empty clientId");
        checkArgument(!isNullOrEmpty(credential), "expected non empty credential");
        checkArgument(!isNullOrEmpty(refreshUrl), "expected non empty refreshUrl");
        checkArgument(!isNullOrEmpty(testDirectory), "expected non empty testDirectory");

        this.dataLakeName = dataLakeName;
        this.clientId = clientId;
        this.credential = credential;
        this.refreshUrl = refreshUrl;
        this.testDirectory = testDirectory;

        super.setup(host, port, databaseName, createHdfsConfiguration());
    }

    private HdfsConfiguration createHdfsConfiguration()
    {
        ConfigurationInitializer azureConfig = new TrinoAzureConfigurationInitializer(new HiveAzureConfig()
                .setAdlClientId(clientId)
                .setAdlCredential(credential)
                .setAdlRefreshUrl(refreshUrl));
        return new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(new HdfsConfig(), ImmutableSet.of(azureConfig)), ImmutableSet.of());
    }

    @Override
    protected Path getBasePath()
    {
        return new Path(format("adl://%s.azuredatalakestore.net/%s/", dataLakeName, testDirectory));
    }

    @Override
    @Test
    public void testRename()
            throws Exception
    {
        Path basePath = new Path(getBasePath(), UUID.randomUUID().toString());
        FileSystem fs = hdfsEnvironment.getFileSystem(TESTING_CONTEXT, basePath);
        assertThat(fs.exists(basePath)).isFalse();

        // create file foo.txt
        Path path = new Path(basePath, "foo.txt");
        assertThat(fs.createNewFile(path)).isTrue();
        assertThat(fs.exists(path)).isTrue();

        // rename foo.txt to bar.txt when bar does not exist
        Path newPath = new Path(basePath, "bar.txt");
        assertThat(fs.exists(newPath)).isFalse();
        assertThat(fs.rename(path, newPath)).isTrue();
        assertThat(fs.exists(path)).isFalse();
        assertThat(fs.exists(newPath)).isTrue();

        // rename foo.txt to foo.txt when foo.txt does not exist
        // This fails with error no such file in ADLFileSystem
        assertThatThrownBy(() -> fs.rename(path, path))
                .isInstanceOf(FileNotFoundException.class);

        // create file foo.txt and rename to existing bar.txt
        assertThat(fs.createNewFile(path)).isTrue();
        assertThat(fs.rename(path, newPath)).isFalse();

        // rename foo.txt to foo.txt when foo.txt exists
        // This returns true in ADLFileSystem
        assertThat(fs.rename(path, path)).isTrue();

        // delete foo.txt
        assertThat(fs.delete(path, false)).isTrue();
        assertThat(fs.exists(path)).isFalse();

        // create directory source with file
        Path source = new Path(basePath, "source");
        assertThat(fs.createNewFile(new Path(source, "test.txt"))).isTrue();

        // rename source to non-existing target
        Path target = new Path(basePath, "target");
        assertThat(fs.exists(target)).isFalse();
        assertThat(fs.rename(source, target)).isTrue();
        assertThat(fs.exists(source)).isFalse();
        assertThat(fs.exists(target)).isTrue();

        // create directory source with file
        assertThat(fs.createNewFile(new Path(source, "test.txt"))).isTrue();

        // rename source to existing target
        assertThat(fs.rename(source, target)).isTrue();
        assertThat(fs.exists(source)).isFalse();
        target = new Path(target, "source");
        assertThat(fs.exists(target)).isTrue();
        assertThat(fs.exists(new Path(target, "test.txt"))).isTrue();

        // delete target
        target = new Path(basePath, "target");
        assertThat(fs.exists(target)).isTrue();
        assertThat(fs.delete(target, true)).isTrue();
        assertThat(fs.exists(target)).isFalse();

        // cleanup
        fs.delete(basePath, true);
    }
}
