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
package io.trino.metastore.polaris;

import com.google.common.io.CountingOutputStream;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;

import static java.util.Objects.requireNonNull;

/**
 * Forwarding OutputFile implementation that delegates to Trino's TrinoOutputFile.
 */
public class ForwardingOutputFile
        implements OutputFile
{
    private final TrinoFileSystem fileSystem;
    private final TrinoOutputFile outputFile;

    public ForwardingOutputFile(TrinoFileSystem fileSystem, Location location)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.outputFile = fileSystem.newOutputFile(location);
    }

    @Override
    public PositionOutputStream create()
    {
        try {
            // Callers of this method don't have access to memory context, so we skip tracking memory here
            return new CountingPositionOutputStream(outputFile.create());
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to create file: " + location(), e);
        }
    }

    @Override
    public PositionOutputStream createOrOverwrite()
    {
        // Iceberg never overwrites existing files. All callers use unique names.
        return create();
    }

    @Override
    public String location()
    {
        return outputFile.location().toString();
    }

    @Override
    public InputFile toInputFile()
    {
        return new ForwardingInputFile(fileSystem.newInputFile(outputFile.location()));
    }

    @Override
    public String toString()
    {
        return outputFile.toString();
    }

    private static class CountingPositionOutputStream
            extends PositionOutputStream
    {
        private final CountingOutputStream stream;

        private CountingPositionOutputStream(OutputStream stream)
        {
            this.stream = new CountingOutputStream(stream);
        }

        @Override
        public long getPos()
        {
            return stream.getCount();
        }

        @Override
        public void write(int b)
                throws IOException
        {
            stream.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len)
                throws IOException
        {
            stream.write(b, off, len);
        }

        @Override
        public void flush()
                throws IOException
        {
            stream.flush();
        }

        @Override
        public void close()
                throws IOException
        {
            stream.close();
        }
    }
}
