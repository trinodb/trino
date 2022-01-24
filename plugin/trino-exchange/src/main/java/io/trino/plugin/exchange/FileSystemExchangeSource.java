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
import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSource
        implements ExchangeSource
{
    private final FileSystemExchangeStorage exchangeStorage;
    @GuardedBy("this")
    private final Iterator<URI> files;
    @GuardedBy("this")
    private final Iterator<Optional<SecretKey>> secretKeys;

    @GuardedBy("this")
    private SliceInput sliceInput;
    @GuardedBy("this")
    private boolean closed;

    public FileSystemExchangeSource(FileSystemExchangeStorage exchangeStorage, List<URI> files, List<Optional<SecretKey>> secretKeys)
    {
        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        checkArgument(requireNonNull(files, "files is null").size() == requireNonNull(secretKeys, "secretKeys is null").size(),
                format("number of files (%d) doesn't match number of secretKeys (%d)", files.size(), secretKeys.size()));
        this.files = ImmutableList.copyOf(files).iterator();
        this.secretKeys = ImmutableList.copyOf(secretKeys).iterator();
    }

    @Override
    public CompletableFuture<Void> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public synchronized boolean isFinished()
    {
        return closed || (!files.hasNext() && sliceInput == null);
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
            if (files.hasNext()) {
                // TODO: implement parallel read
                URI file = files.next();
                Optional<SecretKey> secretKey = secretKeys.next();
                try {
                    sliceInput = exchangeStorage.getSliceInput(file, secretKey);
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
