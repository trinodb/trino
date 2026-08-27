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
package io.trino.plugin.paimon.format;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.plugin.paimon.PaimonPageSourceProvider;
import io.trino.plugin.paimon.PaimonRow;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.format.FormatReaderFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.SeekableInputStream;
import org.apache.paimon.reader.FileRecordIterator;
import org.apache.paimon.reader.FileRecordReader;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.RoaringBitmap32;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.time.Instant;
import java.util.List;

import static java.lang.Math.toIntExact;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

class TrinoPaimonFormatReaderFactory
        implements FormatReaderFactory
{
    private final String formatIdentifier;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final List<DataType> logicalTypes;

    TrinoPaimonFormatReaderFactory(String formatIdentifier, RowType projectedRowType)
    {
        this.formatIdentifier = requireNonNull(formatIdentifier, "formatIdentifier is null");
        requireNonNull(projectedRowType, "projectedRowType is null");
        this.columnNames = List.copyOf(projectedRowType.getFieldNames());
        this.columnTypes = List.copyOf(TrinoPaimonFileFormat.trinoTypes(projectedRowType));
        this.logicalTypes = projectedRowType.getFields().stream()
                .map(field -> field.type())
                .toList();
    }

    @Override
    public FileRecordReader<InternalRow> createReader(Context context)
            throws IOException
    {
        requireNonNull(context, "context is null");
        TrinoInputFile inputFile = new PaimonTrinoInputFile(context.fileIO(), context.filePath(), context.fileSize());
        ConnectorPageSource pageSource = PaimonPageSourceProvider.createNativeDataPageSource(
                formatIdentifier,
                inputFile,
                columnNames,
                columnTypes,
                columnNames.stream().map(_ -> (Domain) null).toList());
        return new TrinoPaimonFormatReader(
                pageSource,
                columnTypes,
                logicalTypes,
                context.filePath(),
                context.selection());
    }

    private static class TrinoPaimonFormatReader
            implements FileRecordReader<InternalRow>
    {
        private final ConnectorPageSource pageSource;
        private final List<Type> columnTypes;
        private final List<DataType> logicalTypes;
        private final Path filePath;
        @Nullable
        private final RoaringBitmap32 selection;

        private Page currentPage;
        private int position;
        private long nextFilePosition;

        private TrinoPaimonFormatReader(
                ConnectorPageSource pageSource,
                List<Type> columnTypes,
                List<DataType> logicalTypes,
                Path filePath,
                @Nullable RoaringBitmap32 selection)
        {
            this.pageSource = requireNonNull(pageSource, "pageSource is null");
            this.columnTypes = List.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
            this.logicalTypes = List.copyOf(requireNonNull(logicalTypes, "logicalTypes is null"));
            this.filePath = requireNonNull(filePath, "filePath is null");
            this.selection = selection;
        }

        @Nullable
        @Override
        public FileRecordIterator<InternalRow> readBatch()
        {
            while (true) {
                if (selectionExhausted()) {
                    return null;
                }

                if (currentPage == null || position >= currentPage.getPositionCount()) {
                    currentPage = nextPage();
                    position = 0;
                }
                if (currentPage == null) {
                    return null;
                }
                if (currentPage.getPositionCount() == 0) {
                    currentPage = null;
                    continue;
                }
                if (remainingPageMatchesSelection()) {
                    return new TrinoPaimonFileRecordIterator(currentPage);
                }
                skipRemainingPage();
            }
        }

        @Override
        public void close()
                throws IOException
        {
            pageSource.close();
        }

        @Nullable
        private Page nextPage()
        {
            SourcePage sourcePage;
            while ((sourcePage = pageSource.getNextSourcePage()) == null) {
                if (pageSource.isFinished()) {
                    return null;
                }
                pageSource.isBlocked().join();
            }
            return sourcePage.getPage();
        }

        private boolean remainingPageMatchesSelection()
        {
            if (selection == null) {
                return true;
            }
            long remainingPositions = currentPage.getPositionCount() - position;
            return selection.intersects(nextFilePosition, nextFilePosition + remainingPositions);
        }

        private boolean selectionExhausted()
        {
            return selection != null
                    && (selection.isEmpty() || nextFilePosition > selection.last());
        }

        private void skipRemainingPage()
        {
            nextFilePosition += currentPage.getPositionCount() - position;
            position = currentPage.getPositionCount();
            currentPage = null;
        }

        private boolean isSelected(long filePosition)
        {
            return selection == null
                    || (filePosition <= RoaringBitmap32.MAX_VALUE && selection.contains(toIntExact(filePosition)));
        }

        private class TrinoPaimonFileRecordIterator
                implements FileRecordIterator<InternalRow>
        {
            private final Page page;
            private long returnedPosition;

            private TrinoPaimonFileRecordIterator(Page page)
            {
                this.page = requireNonNull(page, "page is null");
                this.returnedPosition = -1;
            }

            @Override
            public long returnedPosition()
            {
                if (returnedPosition < 0) {
                    throw new IllegalStateException("returnedPosition() is called before next()");
                }
                return returnedPosition;
            }

            @Override
            public Path filePath()
            {
                return filePath;
            }

            @Nullable
            @Override
            public InternalRow next()
            {
                while (position < page.getPositionCount()) {
                    long filePosition = nextFilePosition++;
                    int pagePosition = position++;
                    if (isSelected(filePosition)) {
                        returnedPosition = filePosition;
                        return PaimonRow.fromTrustedTypeLists(
                                page,
                                pagePosition,
                                RowKind.INSERT,
                                columnTypes,
                                logicalTypes);
                    }
                }
                return null;
            }

            @Override
            public void releaseBatch()
            {
                if (currentPage == page && position < page.getPositionCount()) {
                    nextFilePosition += page.getPositionCount() - position;
                    position = page.getPositionCount();
                }
            }
        }
    }

    private static class PaimonTrinoInputFile
            implements TrinoInputFile
    {
        private final FileIO fileIO;
        private final Path path;
        private final long length;

        private PaimonTrinoInputFile(FileIO fileIO, Path path, long length)
        {
            this.fileIO = requireNonNull(fileIO, "fileIO is null");
            this.path = requireNonNull(path, "path is null");
            this.length = length;
        }

        @Override
        public TrinoInput newInput()
        {
            return new PaimonTrinoInput(fileIO, path, length);
        }

        @Override
        public TrinoInputStream newStream()
                throws IOException
        {
            return new PaimonTrinoInputStream(fileIO.newInputStream(path));
        }

        @Override
        public long length()
        {
            return length;
        }

        @Override
        public Instant lastModified()
                throws IOException
        {
            FileStatus fileStatus = fileIO.getFileStatus(path);
            return Instant.ofEpochMilli(fileStatus.getModificationTime());
        }

        @Override
        public boolean exists()
                throws IOException
        {
            return fileIO.exists(path);
        }

        @Override
        public Location location()
        {
            return Location.of(path.toString());
        }
    }

    private static class PaimonTrinoInput
            implements TrinoInput
    {
        private final FileIO fileIO;
        private final Path path;
        private final long length;
        @Nullable
        private SeekableInputStream inputStream;
        private boolean closed;

        private PaimonTrinoInput(FileIO fileIO, Path path, long length)
        {
            this.fileIO = requireNonNull(fileIO, "fileIO is null");
            this.path = requireNonNull(path, "path is null");
            this.length = length;
        }

        @Override
        public synchronized void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            ensureOpen();
            if (position < 0) {
                throw new IOException("Negative seek offset");
            }
            checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
            if (bufferLength == 0) {
                return;
            }

            SeekableInputStream input = inputStream();
            input.seek(position);
            int read = input.readNBytes(buffer, bufferOffset, bufferLength);
            if (read != bufferLength) {
                throw new EOFException("Cannot read %s bytes at %s. File size is %s: %s"
                        .formatted(bufferLength, position, length, path));
            }
        }

        @Override
        public synchronized int readTail(byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            ensureOpen();
            checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
            if (bufferLength == 0) {
                return 0;
            }

            int readSize = toIntExact(Math.min(length, bufferLength));
            readFully(length - readSize, buffer, bufferOffset, readSize);
            return readSize;
        }

        @Override
        public synchronized void close()
                throws IOException
        {
            if (closed) {
                return;
            }
            closed = true;
            if (inputStream != null) {
                inputStream.close();
                inputStream = null;
            }
        }

        private void ensureOpen()
                throws IOException
        {
            if (closed) {
                throw new IOException("Input closed: " + path);
            }
        }

        private SeekableInputStream inputStream()
                throws IOException
        {
            if (inputStream == null) {
                inputStream = fileIO.newInputStream(path);
            }
            return inputStream;
        }
    }

    private static class PaimonTrinoInputStream
            extends TrinoInputStream
    {
        private final SeekableInputStream inputStream;

        private PaimonTrinoInputStream(SeekableInputStream inputStream)
        {
            this.inputStream = requireNonNull(inputStream, "inputStream is null");
        }

        @Override
        public long getPosition()
                throws IOException
        {
            return inputStream.getPos();
        }

        @Override
        public void seek(long position)
                throws IOException
        {
            inputStream.seek(position);
        }

        @Override
        public int read()
                throws IOException
        {
            return inputStream.read();
        }

        @Override
        public int read(byte[] buffer, int offset, int length)
                throws IOException
        {
            return inputStream.read(buffer, offset, length);
        }

        @Override
        public void close()
                throws IOException
        {
            inputStream.close();
        }
    }
}
