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
package io.trino.plugin.iceberg.fileio;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import org.apache.iceberg.io.InputFile;

import java.io.IOException;
import java.time.Instant;

import static java.util.Objects.requireNonNull;

public class EncryptedTrinoInputFile
        implements TrinoInputFile
{
    private final TrinoInputFile delegate;
    private final InputFile decryptedInputFile;
    private final long length;

    public EncryptedTrinoInputFile(TrinoInputFile delegate, InputFile decryptedInputFile)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.decryptedInputFile = requireNonNull(decryptedInputFile, "decryptedInputFile is null");
        this.length = decryptedInputFile.getLength();
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        return new EncryptedTrinoInput(decryptedInputFile);
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        return new EncryptedTrinoInputStream(decryptedInputFile.newStream());
    }

    @Override
    public long length()
            throws IOException
    {
        return length;
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        return delegate.lastModified();
    }

    @Override
    public boolean exists()
            throws IOException
    {
        return delegate.exists();
    }

    @Override
    public Location location()
    {
        return delegate.location();
    }
}
