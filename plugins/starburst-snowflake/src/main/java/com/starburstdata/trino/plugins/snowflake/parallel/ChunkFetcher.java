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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class ChunkFetcher
{
    private final StarburstResultStreamProvider streamProvider;
    private final Chunk chunk;
    private final ExecutorService executor;
    private long readTimeNanos;
    private CompletableFuture<byte[]> future;

    public ChunkFetcher(StarburstResultStreamProvider streamProvider, Chunk chunk)
    {
        this.streamProvider = requireNonNull(streamProvider, "streamProvider is null");
        this.chunk = requireNonNull(chunk, "chunk is null");
        this.executor = newSingleThreadExecutor(threadsNamed("snowflake-chunk-file-fetcher-%s"));
    }

    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    public boolean startedFetching()
    {
        return future != null;
    }

    public CompletableFuture<byte[]> startFetching()
    {
        checkState(future == null, "future is not null at the beginning of fetching");
        future = CompletableFuture.supplyAsync(() -> {
            long start = System.nanoTime();
            byte[] data = chunk.getInputStream(streamProvider);
            readTimeNanos = System.nanoTime() - start;
            return data;
        }, executor);
        return future;
    }
}
