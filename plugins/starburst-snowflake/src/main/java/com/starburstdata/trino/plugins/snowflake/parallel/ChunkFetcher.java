/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.parallel;

import com.google.common.collect.ImmutableList;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Verify.verify;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class ChunkFetcher
        implements AutoCloseable
{
    private final StarburstResultStreamProvider streamProvider;
    private final ExecutorService executor;
    private final Iterator<Chunk> chunks;
    private long readTimeNanos;
    private CompletableFuture<byte[]> chunkFuture;

    public ChunkFetcher(StarburstResultStreamProvider streamProvider, List<Chunk> chunks)
    {
        this.streamProvider = requireNonNull(streamProvider, "streamProvider is null");
        verify(!requireNonNull(chunks, "chunks is null").isEmpty(), "chunks must not be empty");
        this.chunks = ImmutableList.copyOf(chunks).iterator();
        this.executor = newSingleThreadExecutor(threadsNamed("snowflake-chunk-file-fetcher-%s"));
    }

    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    public boolean isDone()
    {
        return chunkFuture != null && chunkFuture.isDone() && !chunks.hasNext();
    }

    public CompletableFuture<byte[]> fetchNextChunk()
    {
        if (!chunks.hasNext()) {
            return null;
        }

        if (chunkFuture == null || chunkFuture.isDone()) {
            chunkFuture = CompletableFuture.supplyAsync(() -> {
                long start = System.nanoTime();
                byte[] data = chunks.next().getInputStream(streamProvider);
                readTimeNanos += System.nanoTime() - start;
                return data;
            }, executor);
        }

        return chunkFuture;
    }

    @Override
    public void close()
    {
        if (chunkFuture != null) {
            chunkFuture.cancel(true);
            chunkFuture = null;
        }
    }
}
