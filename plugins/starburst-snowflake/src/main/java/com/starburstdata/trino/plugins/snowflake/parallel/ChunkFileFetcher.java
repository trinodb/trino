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

import io.trino.spi.TrinoException;
import net.snowflake.client.jdbc.internal.apache.arrow.memory.BufferAllocator;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.FieldVector;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.ValueVector;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.VectorSchemaRoot;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.ipc.ArrowStreamReader;
import net.snowflake.client.jdbc.internal.apache.arrow.vector.util.TransferPair;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Objects.requireNonNull;

public class ChunkFileFetcher
{
    private final StarburstResultStreamProvider starburstResultStreamProvider;
    private final BufferAllocator bufferAllocator;
    private final SnowflakeArrowSplit split;
    private long readTimeNanos;
    private CompletableFuture<List<List<ValueVector>>> future;

    public ChunkFileFetcher(
            StarburstResultStreamProvider starburstResultStreamProvider,
            BufferAllocator bufferAllocator,
            SnowflakeArrowSplit split)
    {
        this.starburstResultStreamProvider = starburstResultStreamProvider;
        this.bufferAllocator = bufferAllocator;
        this.split = split;
    }

    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    public boolean startedFetching()
    {
        return future != null;
    }

    public CompletableFuture<List<List<ValueVector>>> startFetching()
    {
        future = CompletableFuture.supplyAsync(() -> {
            try {
                long start = System.nanoTime();
                InputStream inputStream = split.getInputStream(requireNonNull(starburstResultStreamProvider, "starburstResultStreamProvider is null"));
                List<List<ValueVector>> batchOfVectors = readArrowStream(inputStream);
                readTimeNanos = System.nanoTime() - start;
                return batchOfVectors;
            }
            catch (IOException e) {
                throw new TrinoException(JDBC_ERROR, "Failed reading Arrow stream", e);
            }
        });
        return future;
    }

    private List<List<ValueVector>> readArrowStream(InputStream is)
            throws IOException
    {
        List<List<ValueVector>> batchOfVectors = new ArrayList<>();
        try (ArrowStreamReader reader = new ArrowStreamReader(is, bufferAllocator)) {
            VectorSchemaRoot vectorSchemaRoot = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                ArrayList<ValueVector> valueVectors = new ArrayList<>();
                for (FieldVector fieldVector : vectorSchemaRoot.getFieldVectors()) {
                    // transfer will not copy data but transfer ownership of memory, otherwise values will be gone
                    // once reader is gone
                    TransferPair transferPair = fieldVector.getTransferPair(bufferAllocator);
                    transferPair.transfer();
                    valueVectors.add(transferPair.getTo());
                }
                batchOfVectors.add(valueVectors);
                vectorSchemaRoot.clear();
            }
            return batchOfVectors;
        }
    }
}
