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
package io.trino.plugin.deltalake.kernel.engine;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.JsonHandler;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.deltalake.kernel.DataTypeJsonSerDe;
import io.trino.plugin.deltalake.kernel.data.JsonRowParser;
import io.trino.plugin.deltalake.kernel.data.TrinoColumnarBatchWrapper;
import io.trino.spi.Page;
import io.trino.spi.type.TypeManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TrinoJsonHandler
        implements JsonHandler
{
    private final TrinoFileSystem fileSystem;
    private final TypeManager typeManager;

    public TrinoJsonHandler(TrinoFileSystem fileSystem, TypeManager typeManager)
    {
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ColumnarBatch parseJson(ColumnVector jsonStringVector, StructType outputSchema, Optional<ColumnVector> selectionVector)
    {
        // TODO: This will be needed for data skipping
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public StructType deserializeStructType(String structTypeJson)
    {
        return DataTypeJsonSerDe.deserializeStructType(structTypeJson);
    }

    @Override
    public CloseableIterator<ColumnarBatch> readJsonFiles(CloseableIterator<FileStatus> fileIter, StructType physicalSchema, Optional<Predicate> predicate)
            throws IOException
    {
        return new CloseableIterator<>()
        {
            private FileStatus currentFile;
            private BufferedReader currentFileReader;
            private String nextLine;

            @Override
            public void close()
                    throws IOException
            {
                Utils.closeCloseables(currentFileReader, fileIter);
            }

            @Override
            public boolean hasNext()
            {
                if (nextLine != null) {
                    return true; // we have un-consumed last read line
                }

                // There is no file in reading or the current file being read has no more data
                // initialize the next file reader or return false if there are no more files to
                // read.
                try {
                    if (currentFileReader == null) {
                        // `nextLine` will initially be null because `currentFileReader` is guaranteed
                        // to be null
                        if (tryOpenNextFile()) {
                            return hasNext();
                        }
                        return false;
                    }
                    nextLine = currentFileReader.readLine();
                    if (nextLine == null) {
                        if (tryOpenNextFile()) {
                            return hasNext();
                        }
                        return false;
                    }

                    return true;
                }
                catch (IOException ex) {
                    throw new UncheckedIOException(
                            format("Error reading JSON file: %s", currentFile.getPath()), ex);
                }
            }

            @Override
            public ColumnarBatch next()
            {
                if (nextLine == null) {
                    throw new NoSuchElementException();
                }

                Page page = JsonRowParser.parseJsonRow(nextLine, physicalSchema, typeManager);
                nextLine = null;
                return new TrinoColumnarBatchWrapper(physicalSchema, page);
            }

            private boolean tryOpenNextFile()
                    throws IOException
            {
                Utils.closeCloseables(currentFileReader); // close the current opened file
                currentFileReader = null;

                if (fileIter.hasNext()) {
                    currentFile = fileIter.next();
                    TrinoInputFile trinoInputFile = fileSystem.newInputFile(Location.of(currentFile.getPath()));
                    currentFileReader = new BufferedReader(new InputStreamReader(trinoInputFile.newStream(), StandardCharsets.UTF_8));
                }
                return currentFileReader != null;
            }
        };
    }

    @Override
    public void writeJsonFileAtomically(String filePath, CloseableIterator<Row> data, boolean overwrite)
            throws IOException
    {
        throw new UnsupportedOperationException("Not implemented");
    }
}
