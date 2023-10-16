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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ChunkFileFetcher
{
    private final StarburstResultStreamProvider streamProvider;
    private final Chunk chunk;
    private long readTimeNanos;
    private CompletableFuture<byte[]> future;

    public ChunkFileFetcher(StarburstResultStreamProvider streamProvider, Chunk chunk)
    {
        this.streamProvider = requireNonNull(streamProvider, "streamProvider is null");
        this.chunk = requireNonNull(chunk, "chunk is null");
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
        });
        return future;
    }
}
