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
package io.trino.plugin.hive.ion;

import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.google.common.io.CountingOutputStream;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.hive.formats.ion.IonEncoder;
import io.trino.hive.formats.ion.IonEncoderFactory;
import io.trino.hive.formats.line.Column;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.plugin.hive.FileWriter;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TypeManager;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;

public class IonFileWriter
        implements FileWriter
{
    private final AggregatedMemoryContext outputStreamMemoryContext;
    private final Closeable rollbackAction;
    private final IonEncoder pageEncoder;
    private final IonWriter writer;
    private final OutputStream outputStream;
    private final LongSupplier bytesWritten;

    public IonFileWriter(
            OutputStream outputStream,
            AggregatedMemoryContext outputStreamMemoryContext,
            Closeable rollbackAction,
            TypeManager typeManager,
            Optional<CompressionKind> compressionKind,
            List<Column> columns)
            throws IOException
    {
        this.outputStreamMemoryContext = outputStreamMemoryContext;
        this.rollbackAction = rollbackAction;
        this.pageEncoder = IonEncoderFactory.buildEncoder(columns);
        CountingOutputStream countingOutputStream = new CountingOutputStream(outputStream);
        this.bytesWritten = countingOutputStream::getCount;
        if (compressionKind.isPresent()) {
            this.outputStream = compressionKind.get().createCodec()
                    .createStreamCompressor(countingOutputStream);
        }
        else {
            this.outputStream = countingOutputStream;
        }

        this.writer = IonTextWriterBuilder
                .minimal()
                .build(this.outputStream);
    }

    @Override
    public long getWrittenBytes()
    {
        return bytesWritten.getAsLong();
    }

    @Override
    public long getMemoryUsage()
    {
        return outputStreamMemoryContext.getBytes();
    }

    @Override
    public Closeable commit()
    {
        try {
            writer.close();
        }
        catch (Exception e) {
            try {
                rollbackAction.close();
            }
            catch (Exception _) {
                // ignore
            }
            throw new TrinoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
        }
        return rollbackAction;
    }

    @Override
    public void rollback()
    {
        try (rollbackAction) {
            writer.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return 0;
    }

    @Override
    public void appendRows(Page page)
    {
        try {
            pageEncoder.encode(writer, page);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
