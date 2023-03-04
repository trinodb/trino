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
package io.trino.plugin.deltalake;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multiset;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.SeekableInputStream;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public final class AccessTrackingFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final TrinoFileSystemFactory delegate;
    private final Multiset<String> openedFiles = HashMultiset.create();

    public AccessTrackingFileSystemFactory(TrinoFileSystemFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new TrackingFileSystem(delegate.create(identity), this::incrementOpenCount);
    }

    public Map<String, Integer> getOpenCount()
    {
        ImmutableMap.Builder<String, Integer> map = ImmutableMap.builder();
        openedFiles.forEachEntry(map::put);
        return map.buildOrThrow();
    }

    private void incrementOpenCount(String path)
    {
        openedFiles.add(path.substring(path.lastIndexOf('/') + 1));
    }

    private static class TrackingFileSystem
            implements TrinoFileSystem
    {
        private final TrinoFileSystem delegate;
        private final Consumer<String> fileOpened;

        public TrackingFileSystem(TrinoFileSystem delegate, Consumer<String> fileOpened)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.fileOpened = requireNonNull(fileOpened, "fileOpened is null");
        }

        @Override
        public TrinoInputFile newInputFile(String path)
        {
            TrinoInputFile inputFile = delegate.newInputFile(path);
            return new TrackingInputFile(inputFile, fileOpened);
        }

        @Override
        public TrinoInputFile newInputFile(String path, long length)
        {
            TrinoInputFile inputFile = delegate.newInputFile(path, length);
            return new TrackingInputFile(inputFile, fileOpened);
        }

        @Override
        public TrinoOutputFile newOutputFile(String path)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String path)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFiles(Collection<String> paths)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteDirectory(String path)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void renameFile(String source, String target)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public FileIterator listFiles(String path)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class TrackingInputFile
            implements TrinoInputFile
    {
        private final TrinoInputFile delegate;
        private final Consumer<String> fileOpened;

        private TrackingInputFile(TrinoInputFile delegate, Consumer<String> fileOpened)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.fileOpened = requireNonNull(fileOpened, "fileOpened is null");
        }

        @Override
        public TrinoInput newInput()
                throws IOException
        {
            fileOpened.accept(location());
            return delegate.newInput();
        }

        @Override
        public SeekableInputStream newStream()
                throws IOException
        {
            fileOpened.accept(location());
            return delegate.newStream();
        }

        @Override
        public long length()
                throws IOException
        {
            return delegate.length();
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
        public String location()
        {
            return delegate.location();
        }

        @Override
        public String toString()
        {
            return delegate.toString();
        }
    }
}
