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

import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import io.trino.hive.formats.ion.IonDecoder;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.function.LongSupplier;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class IonPageSource
        implements ConnectorPageSource
{
    private static final int INSTANCE_SIZE = instanceSize(IonPageSource.class);

    private final IonReader ionReader;
    private final LongSupplier completedBytes;
    private final PageBuilder pageBuilder;
    private final IonDecoder decoder;
    private long readTimeNanos;
    private int completedPositions;
    private boolean finished;

    public IonPageSource(IonReader ionReader, LongSupplier completedBytes, IonDecoder decoder)
    {
        this.ionReader = requireNonNull(ionReader);
        this.completedBytes = requireNonNull(completedBytes);
        this.decoder = requireNonNull(decoder);
        this.pageBuilder = new PageBuilder(decoder.getColumnTypes());
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes.getAsLong();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        while (!pageBuilder.isFull()) {
            if (!readNextValue()) {
                finished = true;
                break;
            }
        }

        Page page = pageBuilder.build();
        completedPositions += page.getPositionCount();
        pageBuilder.reset();
        return SourcePage.create(page);
    }

    @Override
    public long getMemoryUsage()
    {
        // we don't have the ability to ask an IonReader how many bytes it has buffered
        // it will buffer as much as is needed for each top-level-value.
        int assumedIonBufferSize = IonBufferConfiguration.DEFAULT.getInitialBufferSize() * 4;
        return INSTANCE_SIZE + assumedIonBufferSize + pageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public void close()
            throws IOException
    {
        ionReader.close();
    }

    private boolean readNextValue()
    {
        long start = System.nanoTime();
        IonType type = ionReader.next();

        if (type == null) {
            readTimeNanos += System.nanoTime() - start;
            return false;
        }
        decoder.decode(ionReader, pageBuilder);

        readTimeNanos += System.nanoTime() - start;
        return true;
    }
}
