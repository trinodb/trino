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
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hive.fs.HiveFileIterator.containsHiddenPathPartAfterIndex;
import static io.trino.plugin.hive.fs.HiveFileIterator.isHiddenFileOrDirectory;
import static io.trino.plugin.hive.fs.HiveFileIterator.isHiddenOrWithinHiddenParentDirectory;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveFileIterator
{
    @Test
    public void testRelativeHiddenPathDetection()
    {
        assertThat(isHiddenOrWithinHiddenParentDirectory(Location.of("file:///root-path/.hidden/child"), Location.of("file:///root-path"))).isTrue();
        assertThat(isHiddenOrWithinHiddenParentDirectory(Location.of("file:///root-path/_hidden.txt"), Location.of("file:///root-path"))).isTrue();

        // root path with trailing slash
        assertThat(isHiddenOrWithinHiddenParentDirectory(Location.of("file:///root-path/.hidden/child"), Location.of("file:///root-path/"))).isTrue();
        assertThat(isHiddenOrWithinHiddenParentDirectory(Location.of("file:///root-path/_hidden.txt"), Location.of("file:///root-path/"))).isTrue();

        // root path containing .hidden
        assertThat(isHiddenOrWithinHiddenParentDirectory(Location.of("file:///root/.hidden/listing-root/file.txt"), Location.of("file:///root/.hidden/listing-root"))).isFalse();

        // root path ending with an underscore
        assertThat(isHiddenOrWithinHiddenParentDirectory(Location.of("file:///root/hidden-ending_/file.txt"), Location.of("file:///root/hidden-ending_"))).isFalse();

        // root path containing "arbitrary" characters
        assertThat(isHiddenOrWithinHiddenParentDirectory(Location.of("file:///root/With spaces and | pipes/.hidden/file.txt"), Location.of("file:///root/With spaces and | pipes/.hidden"))).isFalse();
    }

    @Test
    public void testHiddenFileNameDetection()
    {
        assertThat(isHiddenFileOrDirectory(Location.of("file:///parent/.hidden/ignore-parent-directories.txt"))).isFalse();
        assertThat(isHiddenFileOrDirectory(Location.of("file:///parent/visible/_hidden-file.txt"))).isTrue();
    }

    @Test
    public void testHiddenDetectionEdgeCases()
    {
        assertThat(containsHiddenPathPartAfterIndex("/.leading-slash-hidden/file.txt", 0)).isTrue();
        assertThat(containsHiddenPathPartAfterIndex("/.leading-slash-hidden-directory/", 0)).isTrue();
        assertThat(containsHiddenPathPartAfterIndex("_root-hidden/file.txt", 0)).isTrue();
        assertThat(containsHiddenPathPartAfterIndex("_root-hidden/directory/", 0)).isTrue();
        assertThat(containsHiddenPathPartAfterIndex("root//multi-slash/", 0)).isFalse();
        assertThat(containsHiddenPathPartAfterIndex("root/child/.slash-hidden/file.txt", 0)).isTrue();
        assertThat(containsHiddenPathPartAfterIndex("root/child/.slash-hidden/parent/file.txt", 0)).isTrue();
    }
}
