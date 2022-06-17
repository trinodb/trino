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
package io.trino.plugin.iceberg.io.hdfs;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.io.FileIterator;
import io.trino.plugin.iceberg.io.TrinoFileSystem;
import io.trino.plugin.iceberg.io.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static java.nio.file.Files.createDirectory;
import static java.nio.file.Files.createFile;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHdfsFileSystem
{
    @Test
    public void testListing()
            throws IOException
    {
        TrinoFileSystemFactory factory = new HdfsFileSystemFactory(HDFS_ENVIRONMENT);
        TrinoFileSystem fileSystem = factory.create(ConnectorIdentity.ofUser("test"));

        Path tempDir = createTempDirectory("testListing");
        String root = tempDir.toString();

        assertThat(listFiles(fileSystem, root)).isEmpty();

        createFile(tempDir.resolve("abc"));
        createFile(tempDir.resolve("xyz"));
        createDirectory(tempDir.resolve("mydir"));

        assertThat(listFiles(fileSystem, root)).containsExactlyInAnyOrder(
                root + "/abc",
                root + "/xyz");

        assertThat(listFiles(fileSystem, root + "/abc")).containsExactly(root + "/abc");
        assertThat(listFiles(fileSystem, root + "/abc/")).containsExactly(root + "/abc/");
        assertThat(listFiles(fileSystem, root + "/abc//")).containsExactly(root + "/abc//");
        assertThat(listFiles(fileSystem, root + "///abc")).containsExactly(root + "///abc");

        createFile(tempDir.resolve("mydir").resolve("qqq"));

        assertThat(listFiles(fileSystem, root)).containsExactlyInAnyOrder(
                root + "/abc",
                root + "/xyz",
                root + "/mydir/qqq");

        assertThat(listFiles(fileSystem, root + "/mydir")).containsExactly(root + "/mydir/qqq");
        assertThat(listFiles(fileSystem, root + "/mydir/")).containsExactly(root + "/mydir/qqq");
        assertThat(listFiles(fileSystem, root + "/mydir//")).containsExactly(root + "/mydir//qqq");
        assertThat(listFiles(fileSystem, root + "///mydir")).containsExactly(root + "///mydir/qqq");

        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    private static List<String> listFiles(TrinoFileSystem fileSystem, String path)
            throws IOException
    {
        FileIterator iterator = fileSystem.listFiles(path);
        ImmutableList.Builder<String> files = ImmutableList.builder();
        while (iterator.hasNext()) {
            files.add(iterator.next().path());
        }
        return files.build();
    }
}
