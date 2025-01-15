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
import io.trino.hive.formats.TrinoDataInputStream;
import io.trino.hive.formats.ion.IonDecoder;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.OptionalLong;

import static io.airlift.slice.SizeOf.instanceSize;

public class IonPageSource
        implements ConnectorPageSource
{
    private static final int INSTANCE_SIZE = instanceSize(IonPageSource.class);

    private final TrinoDataInputStream inputStream;
    private final IonReader ionReader;
    private final PageBuilder pageBuilder;
    private final IonDecoder decoder;
    private int completedPositions;
    private boolean finished;

    public IonPageSource(IonReader ionReader, TrinoDataInputStream inputStream, IonDecoder decoder, PageBuilder pageBuilder)
    {
        this.ionReader = ionReader;
        this.inputStream = inputStream;
        this.decoder = decoder;
        this.pageBuilder = pageBuilder;
        this.completedPositions = 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return inputStream.getReadBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos()
    {
        return inputStream.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
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
        return page;
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
        final IonType type = ionReader.next();
        if (type == null) {
            return false;
        }

        pageBuilder.declarePosition();
        decoder.decode(ionReader);
        return true;
    }
}
