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
package io.trino.spiller;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.util.Objects.requireNonNull;

@ThreadSafe
final class SpillFile
        implements Closeable
{
    @VisibleForTesting
    static final int BUFFER_SIZE = 4 * 1024;

    private final Path filePath;

    @GuardedBy("this")
    private boolean deleted;

    @GuardedBy("this")
    private OutputStreamSliceOutput output;

    public SpillFile(Path filePath)
    {
        this.filePath = requireNonNull(filePath, "filePath is null");
    }

    public synchronized void writeBytes(Slice slice)
            throws IOException
    {
        requireNonNull(slice, "slice is null");
        checkState(!deleted, "File already deleted");
        if (output == null) {
            output = new OutputStreamSliceOutput(Files.newOutputStream(filePath, APPEND), BUFFER_SIZE);
        }
        output.writeBytes(slice);
    }

    public synchronized void closeOutput()
            throws IOException
    {
        if (output != null) {
            output.close();
            output = null;
        }
    }

    public synchronized InputStream newInputStream()
            throws IOException
    {
        checkState(!deleted, "File already deleted");
        closeOutput();
        return Files.newInputStream(filePath);
    }

    @Override
    public synchronized void close()
    {
        if (deleted) {
            return;
        }
        deleted = true;

        try {
            closeOutput();
            Files.delete(filePath);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
