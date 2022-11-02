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
package io.trino.filesystem.fileio;

import io.trino.filesystem.TrinoFileSystem;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.io.IOException;
import java.io.UncheckedIOException;

import static java.util.Objects.requireNonNull;

public class ForwardingFileIo
        implements FileIO
{
    private final TrinoFileSystem fileSystem;

    public ForwardingFileIo(TrinoFileSystem fileSystem)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
    }

    @Override
    public InputFile newInputFile(String path)
    {
        return new ForwardingInputFile(fileSystem.newInputFile(path));
    }

    @Override
    public InputFile newInputFile(String path, long length)
    {
        return new ForwardingInputFile(fileSystem.newInputFile(path, length));
    }

    @Override
    public OutputFile newOutputFile(String path)
    {
        return new ForwardingOutputFile(fileSystem, path);
    }

    @Override
    public void deleteFile(String path)
    {
        try {
            fileSystem.deleteFile(path);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to delete file: " + path, e);
        }
    }
}
