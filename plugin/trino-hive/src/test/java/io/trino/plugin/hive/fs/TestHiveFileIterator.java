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

import org.apache.hadoop.fs.Path;
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
        String root = new Path("file:///root-path").toUri().getPath();
        assertTrue(isHiddenOrWithinHiddenParentDirectory(new Path(root, ".hidden/child"), root));
        assertTrue(isHiddenOrWithinHiddenParentDirectory(new Path(root, "_hidden.txt"), root));
        String rootWithinHidden = new Path("file:///root/.hidden/listing-root").toUri().getPath();
        assertFalse(isHiddenOrWithinHiddenParentDirectory(new Path(rootWithinHidden, "file.txt"), rootWithinHidden));
        String rootHiddenEnding = new Path("file:///root/hidden-ending_").toUri().getPath();
        assertFalse(isHiddenOrWithinHiddenParentDirectory(new Path(rootHiddenEnding, "file.txt"), rootHiddenEnding));
    }

    @Test
    public void testHiddenFileNameDetection()
    {
        assertFalse(isHiddenFileOrDirectory(new Path("file:///parent/.hidden/ignore-parent-directories.txt")));
        assertTrue(isHiddenFileOrDirectory(new Path("file:///parent/visible/_hidden-file.txt")));
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
