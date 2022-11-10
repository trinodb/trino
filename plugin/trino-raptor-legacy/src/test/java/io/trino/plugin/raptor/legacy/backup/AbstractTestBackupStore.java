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

import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.UUID;

import static java.nio.file.Files.readAllBytes;
import static java.nio.file.Files.writeString;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

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

        assertFalse(store.shardExists(uuid1));
        store.backupShard(uuid1, file1);
        assertTrue(store.shardExists(uuid1));

        // backup second file
        File file2 = temporary.resolve("file2").toFile();
        writeString(file2.toPath(), "bye bye");
        UUID uuid2 = randomUUID();

        assertFalse(store.shardExists(uuid2));
        store.backupShard(uuid2, file2);
        assertTrue(store.shardExists(uuid2));

        // verify first file
        File restore1 = temporary.resolve("restore1").toFile();
        store.restoreShard(uuid1, restore1);
        assertEquals(readAllBytes(file1.toPath()), readAllBytes(restore1.toPath()));

        // verify second file
        File restore2 = temporary.resolve("restore2").toFile();
        store.restoreShard(uuid2, restore2);
        assertEquals(readAllBytes(file2.toPath()), readAllBytes(restore2.toPath()));

        // verify random UUID does not exist
        assertFalse(store.shardExists(randomUUID()));

        // delete first file
        assertTrue(store.shardExists(uuid1));
        assertTrue(store.shardExists(uuid2));

        store.deleteShard(uuid1);
        store.deleteShard(uuid1);

        assertFalse(store.shardExists(uuid1));
        assertTrue(store.shardExists(uuid2));

        // delete random UUID
        store.deleteShard(randomUUID());
    }
}
