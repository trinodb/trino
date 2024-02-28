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
package io.trino.plugin.raptor.legacy.backup;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.UUID;

import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Files.writeString;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestBackupStore<T extends BackupStore>
{
    protected Path temporary;
    protected T store;

    @Test
    public void testBackupStore()
            throws Exception
    {
        // backup first file
        File file1 = temporary.resolve("file1").toFile();
        writeString(file1.toPath(), "hello world");
        UUID uuid1 = randomUUID();

        assertThat(store.shardExists(uuid1)).isFalse();
        store.backupShard(uuid1, file1);
        assertThat(store.shardExists(uuid1)).isTrue();

        // backup second file
        File file2 = temporary.resolve("file2").toFile();
        writeString(file2.toPath(), "bye bye");
        UUID uuid2 = randomUUID();

        assertThat(store.shardExists(uuid2)).isFalse();
        store.backupShard(uuid2, file2);
        assertThat(store.shardExists(uuid2)).isTrue();

        // verify first file
        File restore1 = temporary.resolve("restore1").toFile();
        store.restoreShard(uuid1, restore1);
        assertThat(readAllBytes(file1.toPath())).isEqualTo(readAllBytes(restore1.toPath()));

        // verify second file
        File restore2 = temporary.resolve("restore2").toFile();
        store.restoreShard(uuid2, restore2);
        assertThat(readAllBytes(file2.toPath())).isEqualTo(readAllBytes(restore2.toPath()));

        // verify random UUID does not exist
        assertThat(store.shardExists(randomUUID())).isFalse();

        // delete first file
        assertThat(store.shardExists(uuid1)).isTrue();
        assertThat(store.shardExists(uuid2)).isTrue();

        store.deleteShard(uuid1);
        store.deleteShard(uuid1);

        assertThat(store.shardExists(uuid1)).isFalse();
        assertThat(store.shardExists(uuid2)).isTrue();

        // delete random UUID
        store.deleteShard(randomUUID());
    }
}
