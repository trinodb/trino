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
package io.trino.plugin.hive.fs;

import io.trino.filesystem.Location;
import org.testng.annotations.Test;

import static io.trino.plugin.hive.fs.HiveFileIterator.containsHiddenPathPartAfterIndex;
import static io.trino.plugin.hive.fs.HiveFileIterator.isHiddenFileOrDirectory;
import static io.trino.plugin.hive.fs.HiveFileIterator.isHiddenOrWithinHiddenParentDirectory;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveFileIterator
{
    @Test
    public void testRelativeHiddenPathDetection()
    {
        assertTrue(isHiddenOrWithinHiddenParentDirectory(Location.of("file:///root-path/.hidden/child"), Location.of("file:///root-path")));
        assertTrue(isHiddenOrWithinHiddenParentDirectory(Location.of("file:///root-path/_hidden.txt"), Location.of("file:///root-path")));

        // root path with trailing slash
        assertTrue(isHiddenOrWithinHiddenParentDirectory(Location.of("file:///root-path/.hidden/child"), Location.of("file:///root-path/")));
        assertTrue(isHiddenOrWithinHiddenParentDirectory(Location.of("file:///root-path/_hidden.txt"), Location.of("file:///root-path/")));

        // root path containing .hidden
        assertFalse(isHiddenOrWithinHiddenParentDirectory(Location.of("file:///root/.hidden/listing-root/file.txt"), Location.of("file:///root/.hidden/listing-root")));

        // root path ending with an underscore
        assertFalse(isHiddenOrWithinHiddenParentDirectory(Location.of("file:///root/hidden-ending_/file.txt"), Location.of("file:///root/hidden-ending_")));

        // root path containing "arbitrary" characters
        assertFalse(isHiddenOrWithinHiddenParentDirectory(Location.of("file:///root/With spaces and | pipes/.hidden/file.txt"), Location.of("file:///root/With spaces and | pipes/.hidden")));
    }

    @Test
    public void testHiddenFileNameDetection()
    {
        assertFalse(isHiddenFileOrDirectory(Location.of("file:///parent/.hidden/ignore-parent-directories.txt")));
        assertTrue(isHiddenFileOrDirectory(Location.of("file:///parent/visible/_hidden-file.txt")));
    }

    @Test
    public void testHiddenDetectionEdgeCases()
    {
        assertTrue(containsHiddenPathPartAfterIndex("/.leading-slash-hidden/file.txt", 0));
        assertTrue(containsHiddenPathPartAfterIndex("/.leading-slash-hidden-directory/", 0));
        assertTrue(containsHiddenPathPartAfterIndex("_root-hidden/file.txt", 0));
        assertTrue(containsHiddenPathPartAfterIndex("_root-hidden/directory/", 0));
        assertFalse(containsHiddenPathPartAfterIndex("root//multi-slash/", 0));
        assertTrue(containsHiddenPathPartAfterIndex("root/child/.slash-hidden/file.txt", 0));
        assertTrue(containsHiddenPathPartAfterIndex("root/child/.slash-hidden/parent/file.txt", 0));
    }
}
