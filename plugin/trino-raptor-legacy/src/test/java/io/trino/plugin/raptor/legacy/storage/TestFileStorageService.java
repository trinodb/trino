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
package io.trino.plugin.raptor.legacy.storage;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.UUID;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.raptor.legacy.storage.FileStorageService.getFileSystemPath;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestFileStorageService
{
    private Path temporary;
    private FileStorageService store;

    @BeforeEach
    public void setup()
            throws IOException
    {
        temporary = createTempDirectory(null);
        store = new FileStorageService(temporary.toFile());
        store.start();
    }

    @AfterEach
    public void tearDown()
            throws Exception
    {
        deleteRecursively(temporary, ALLOW_INSECURE);
    }

    @Test
    public void testGetFileSystemPath()
    {
        UUID uuid = UUID.fromString("701e1a79-74f7-4f56-b438-b41e8e7d019d");
        File expected = new File("/test", format("70/1e/%s.orc", uuid));
        assertThat(getFileSystemPath(new File("/test"), uuid)).isEqualTo(expected);
    }

    @Test
    public void testFilePaths()
    {
        UUID uuid = UUID.fromString("701e1a79-74f7-4f56-b438-b41e8e7d019d");
        File staging = temporary.resolve("staging").resolve(format("%s.orc", uuid)).toFile();
        File storage = temporary.resolve("storage").resolve("70").resolve("1e").resolve(format("%s.orc", uuid)).toFile();
        File quarantine = temporary.resolve("quarantine").resolve(format("%s.orc", uuid)).toFile();
        assertThat(store.getStagingFile(uuid)).isEqualTo(staging);
        assertThat(store.getStorageFile(uuid)).isEqualTo(storage);
        assertThat(store.getQuarantineFile(uuid)).isEqualTo(quarantine);
    }

    @Test
    public void testStop()
            throws Exception
    {
        File staging = temporary.resolve("staging").toFile();
        File storage = temporary.resolve("storage").toFile();
        File quarantine = temporary.resolve("quarantine").toFile();

        assertThat(staging).isDirectory();
        assertThat(storage).isDirectory();
        assertThat(quarantine).isDirectory();

        File file = store.getStagingFile(randomUUID());
        store.createParents(file);
        assertThat(file.exists()).isFalse();
        assertThat(file.createNewFile()).isTrue();
        assertThat(file).isFile();

        store.stop();

        assertThat(file.exists()).isFalse();
        assertThat(staging.exists()).isFalse();
        assertThat(storage).isDirectory();
        assertThat(quarantine).isDirectory();
    }

    @Test
    public void testGetStorageShards()
            throws Exception
    {
        Set<UUID> shards = ImmutableSet.<UUID>builder()
                .add(UUID.fromString("9e7abb51-56b5-4180-9164-ad08ddfe7c63"))
                .add(UUID.fromString("bbfc3895-1c3d-4bf4-bca4-7b1198b1759e"))
                .build();

        for (UUID shard : shards) {
            File file = store.getStorageFile(shard);
            store.createParents(file);
            assertThat(file.createNewFile()).isTrue();
        }

        File storage = temporary.resolve("storage").toFile();
        assertThat(new File(storage, "abc").mkdir()).isTrue();
        assertThat(new File(storage, "ab/cd").mkdirs()).isTrue();
        assertThat(new File(storage, format("ab/cd/%s.junk", randomUUID())).createNewFile()).isTrue();
        assertThat(new File(storage, "ab/cd/junk.orc").createNewFile()).isTrue();

        assertThat(store.getStorageShards()).isEqualTo(shards);
    }
}
