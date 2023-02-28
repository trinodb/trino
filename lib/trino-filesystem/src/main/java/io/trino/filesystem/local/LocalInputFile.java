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
package io.trino.filesystem.local;

import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.File;
import java.io.IOException;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

public class LocalInputFile
        implements TrinoInputFile
{
    private final File file;

    public LocalInputFile(File file)
    {
        this.file = requireNonNull(file, "file is null");
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        return new LocalInput(file);
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        return new FileTrinoInputStream(file);
    }

    @Override
    public long length()
            throws IOException
    {
        return file.length();
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        return Instant.ofEpochMilli(file.lastModified());
    }

    @Override
    public boolean exists()
            throws IOException
    {
        return file.exists();
    }

    @Override
    public String location()
    {
        return file.getPath();
    }

    @Override
    public String toString()
    {
        return location();
    }
}
