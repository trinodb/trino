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
package io.trino.plugin.exchange;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.trino.spi.exchange.ExchangeSource;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSource
        implements ExchangeSource
{
    private final FileSystemExchangeStorage exchangeStorage;
    @GuardedBy("this")
    private final Iterator<ExchangeSourceFile> exchangeSourceFiles;

    @GuardedBy("this")
    private SliceInput sliceInput;
    @GuardedBy("this")
    private boolean closed;

    public FileSystemExchangeSource(FileSystemExchangeStorage exchangeStorage, List<ExchangeSourceFile> exchangeSourceFiles)
    {
        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.exchangeSourceFiles = ImmutableList.copyOf(exchangeSourceFiles).iterator();
    }

    @Override
    public CompletableFuture<Void> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public synchronized boolean isFinished()
    {
        return closed || (!exchangeSourceFiles.hasNext() && sliceInput == null);
    }

    @Nullable
    @Override
    public synchronized Slice read()
    {
        if (isFinished()) {
            return null;
        }

        if (sliceInput != null && !sliceInput.isReadable()) {
            sliceInput.close();
            sliceInput = null;
        }

        if (sliceInput == null) {
            if (exchangeSourceFiles.hasNext()) {
                // TODO: implement parallel read
                ExchangeSourceFile exchangeSourceFile = exchangeSourceFiles.next();
                try {
                    sliceInput = exchangeStorage.getSliceInput(exchangeSourceFile);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        if (sliceInput == null) {
            return null;
        }

        if (!sliceInput.isReadable()) {
            sliceInput.close();
            sliceInput = null;
            return null;
        }

        int size = sliceInput.readInt();
        return sliceInput.readSlice(size);
    }

    @Override
    public synchronized long getMemoryUsage()
    {
        return sliceInput != null ? sliceInput.getRetainedSize() : 0;
    }

    @Override
    public synchronized void close()
    {
        if (!closed) {
            closed = true;
            if (sliceInput != null) {
                sliceInput.close();
                sliceInput = null;
            }
        }
    }
}
