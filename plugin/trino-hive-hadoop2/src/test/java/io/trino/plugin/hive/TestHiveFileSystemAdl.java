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
import io.trino.plugin.hive.azure.HiveAzureConfig;
import io.trino.plugin.hive.azure.TrinoAzureConfigurationInitializer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.util.Strings.isNullOrEmpty;

public class TestHiveFileSystemAdl
        extends AbstractTestHiveFileSystem
{
    private String dataLakeName;
    private String clientId;
    private String credential;
    private String refreshUrl;
    private String testDirectory;

    @Parameters({
            "hive.hadoop2.metastoreHost",
            "hive.hadoop2.metastorePort",
            "hive.hadoop2.databaseName",
            "hive.hadoop2.adl.name",
            "hive.hadoop2.adl.clientId",
            "hive.hadoop2.adl.credential",
            "hive.hadoop2.adl.refreshUrl",
            "hive.hadoop2.adl.testDirectory",
    })
    @BeforeClass
    public void setup(String host, int port, String databaseName, String dataLakeName, String clientId, String credential, String refreshUrl, String testDirectory)
    {
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

        super.setup(host, port, databaseName, false, createHdfsConfiguration());
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
        assertFalse(fs.exists(basePath));

        // create file foo.txt
        Path path = new Path(basePath, "foo.txt");
        assertTrue(fs.createNewFile(path));
        assertTrue(fs.exists(path));

        // rename foo.txt to bar.txt when bar does not exist
        Path newPath = new Path(basePath, "bar.txt");
        assertFalse(fs.exists(newPath));
        assertTrue(fs.rename(path, newPath));
        assertFalse(fs.exists(path));
        assertTrue(fs.exists(newPath));

        // rename foo.txt to foo.txt when foo.txt does not exist
        // This fails with error no such file in ADLFileSystem
        assertThatThrownBy(() -> fs.rename(path, path))
                .isInstanceOf(FileNotFoundException.class);

        // create file foo.txt and rename to existing bar.txt
        assertTrue(fs.createNewFile(path));
        assertFalse(fs.rename(path, newPath));

        // rename foo.txt to foo.txt when foo.txt exists
        // This returns true in ADLFileSystem
        assertTrue(fs.rename(path, path));

        // delete foo.txt
        assertTrue(fs.delete(path, false));
        assertFalse(fs.exists(path));

        // create directory source with file
        Path source = new Path(basePath, "source");
        assertTrue(fs.createNewFile(new Path(source, "test.txt")));

        // rename source to non-existing target
        Path target = new Path(basePath, "target");
        assertFalse(fs.exists(target));
        assertTrue(fs.rename(source, target));
        assertFalse(fs.exists(source));
        assertTrue(fs.exists(target));

        // create directory source with file
        assertTrue(fs.createNewFile(new Path(source, "test.txt")));

        // rename source to existing target
        assertTrue(fs.rename(source, target));
        assertFalse(fs.exists(source));
        target = new Path(target, "source");
        assertTrue(fs.exists(target));
        assertTrue(fs.exists(new Path(target, "test.txt")));

        // delete target
        target = new Path(basePath, "target");
        assertTrue(fs.exists(target));
        assertTrue(fs.delete(target, true));
        assertFalse(fs.exists(target));

        // cleanup
        fs.delete(basePath, true);
    }
}
