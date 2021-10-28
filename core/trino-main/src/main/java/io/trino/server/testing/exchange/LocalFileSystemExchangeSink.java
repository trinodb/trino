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
package io.trino.server.testing.exchange;

import io.airlift.log.Logger;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import io.trino.spi.exchange.ExchangeSink;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.GuardedBy;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.lang.Math.toIntExact;
import static java.nio.file.Files.createFile;
import static java.util.Objects.requireNonNull;

public class LocalFileSystemExchangeSink
        implements ExchangeSink
{
    private static final Logger log = Logger.get(LocalFileSystemExchangeSink.class);

    public static final String COMMITTED_MARKER_FILE_NAME = "committed";
    public static final String DATA_FILE_SUFFIX = ".data";

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LocalFileSystemExchangeSink.class).instanceSize();
    private static final int INTEGER_INSTANCE_SIZE = ClassLayout.parseClass(Integer.class).instanceSize();

    private static final int BUFFER_SIZE_IN_BYTES = toIntExact(DataSize.of(4, KILOBYTE).toBytes());

    private final Path outputDirectory;
    private final int outputPartitionCount;

    @GuardedBy("this")
    private final Map<Integer, SliceOutput> outputs = new HashMap<>();
    @GuardedBy("this")
    private boolean committed;
    @GuardedBy("this")
    private boolean closed;

    public LocalFileSystemExchangeSink(Path outputDirectory, int outputPartitionCount)
    {
        this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
        this.outputPartitionCount = outputPartitionCount;
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public synchronized void add(int partitionId, Slice data)
    {
        checkArgument(partitionId < outputPartitionCount, "partition id is expected to be less than %s: %s", outputPartitionCount, partitionId);
        checkState(!committed, "already committed");
        if (closed) {
            return;
        }
        SliceOutput output = outputs.computeIfAbsent(partitionId, this::createOutput);
        output.writeInt(data.length());
        output.writeBytes(data);
    }

    private SliceOutput createOutput(int partitionId)
    {
        Path outputPath = outputDirectory.resolve(partitionId + DATA_FILE_SUFFIX);
        try {
            return new OutputStreamSliceOutput(new FileOutputStream(outputPath.toFile()), BUFFER_SIZE_IN_BYTES);
        }
        catch (FileNotFoundException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized long getMemoryUsage()
    {
        return INSTANCE_SIZE +
                estimatedSizeOf(outputs, (ignored) -> INTEGER_INSTANCE_SIZE, SliceOutput::getRetainedSize);
    }

    @Override
    public synchronized void finish()
    {
        if (closed) {
            return;
        }
        try {
            for (SliceOutput output : outputs.values()) {
                try {
                    output.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            outputs.clear();
            try {
                createFile(outputDirectory.resolve(COMMITTED_MARKER_FILE_NAME));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        catch (Throwable t) {
            abort();
            throw t;
        }
        committed = true;
        closed = true;
    }

    @Override
    public synchronized void abort()
    {
        if (closed) {
            return;
        }
        closed = true;
        for (SliceOutput output : outputs.values()) {
            try {
                output.close();
            }
            catch (IOException e) {
                log.warn(e, "Error closing output");
            }
        }
        outputs.clear();
        try {
            deleteRecursively(outputDirectory, ALLOW_INSECURE);
        }
        catch (IOException e) {
            log.warn(e, "Error cleaning output directory");
        }
    }
}
