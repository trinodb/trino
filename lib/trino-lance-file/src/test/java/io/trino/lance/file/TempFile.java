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
package io.trino.lance.file;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.nio.file.Files.createTempDirectory;

public class TempFile
        implements Closeable
{
    private final Path tempDir;
    private final File file;

    public TempFile()
            throws IOException
    {
        tempDir = createTempDirectory(null);
        file = tempDir.resolve("data.lance").toFile();
    }

    public File getFile()
    {
        return file;
    }

    @Override
    public void close()
            throws IOException
    {
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }
}
