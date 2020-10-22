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

import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.Paths

// We expect this to run on top of the unpacked tarball, which contains
// hard links for any duplicate named JAR files. All of the JARs are
// moved to a top level shared directory and replaced with symlinks.

Path root = Paths.get(properties['root'])
Path shared = root.resolve('shared')

Files.createDirectory(shared)

List<String> files = new FileNameFinder().getFileNames(root.toString(), '**/*.jar')

for (file in files) {
    Path source = Paths.get(file)
    Path target = shared.resolve(source.getFileName())
    if (!Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
        Files.move(source, target)
    }
    else if (!Files.isSameFile(source, target)) {
        throw new RuntimeException("Not same file: $source <> $target")
    }
    else {
        Files.delete(source)
    }
    Files.createSymbolicLink(source, source.getParent().relativize(target))
}
