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
package io.trino.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.DeferredIterable;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.InlineSegment;
import io.trino.client.spooling.Segment;
import io.trino.client.spooling.SegmentLoader;
import io.trino.client.spooling.SpooledSegment;
import io.trino.client.spooling.encoding.QueryDataDecoders;
import org.gaul.modernizer_maven_annotations.SuppressModernizer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Streams.forEachPair;
import static io.trino.client.ResultRows.NULL_ROWS;
import static io.trino.client.ResultRows.fromIterableRows;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * Class responsible for decoding any QueryData type.
 */
public class ResultRowsDecoder
        implements AutoCloseable
{
    private final SegmentLoader loader;
    private final long prefetchBufferSize;
    private final ExecutorService decoderExecutorService;
    private final ExecutorService segmentLoaderExecutorService;
    private QueryDataDecoder decoder;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition sizeCondition = lock.newCondition();
    private final Deque<Thread> waitingThreads = new ArrayDeque<>();
    private long currentSizeInBytes;

    @VisibleForTesting
    public ResultRowsDecoder()
    {
        this(new OkHttpSegmentLoader(),
                "64000000",
                newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("Decoder-%s").setDaemon(true).build()),
                newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Segment loader worker-%s").setDaemon(true).build()));
    }

    @VisibleForTesting
    public ResultRowsDecoder(SegmentLoader loader)
    {
        this(loader,
                "64000000",
                newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("Decoder-%s").setDaemon(true).build()),
                newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("Segment loader worker-%s").setDaemon(true).build()));
    }

    public ResultRowsDecoder(SegmentLoader loader, String prefetchBufferSize, ExecutorService decoderExecutorService, ExecutorService segmentLoaderExecutorService)
    {
        this.loader = requireNonNull(loader, "loader is null");
        this.prefetchBufferSize = Long.parseLong(requireNonNull(prefetchBufferSize, "prefetchBufferSize is null"));
        this.decoderExecutorService = requireNonNull(decoderExecutorService, "decoder is null");
        this.segmentLoaderExecutorService = requireNonNull(segmentLoaderExecutorService, "segmentLoaderExecutor is null");
    }

    private void setEncoding(List<Column> columns, String encoding)
    {
        if (decoder != null) {
            checkState(decoder.encoding().equals(encoding), "Decoder is configured for encoding %s but got %s", decoder.encoding(), encoding);
        }
        else {
            checkState(!columns.isEmpty(), "Columns must be set when decoding data");
            this.decoder = QueryDataDecoders.get(encoding)
                    // we don't use query-level attributes for now
                    .create(columns, DataAttributes.empty());
        }
    }

    public ResultRows toRows(QueryResults results)
    {
        if (results == null || results.getData() == null) {
            return NULL_ROWS;
        }

        return toRows(results.getColumns(), results.getData());
    }

    public ResultRows toRows(List<Column> columns, QueryData data)
    {
        if (data == null || data.isNull()) {
            return NULL_ROWS; // for backward compatibility instead of null
        }

        verify(columns != null && !columns.isEmpty(), "Columns must be set when decoding data");
        if (data instanceof TypedQueryData) {
            TypedQueryData rawData = (TypedQueryData) data;
            if (rawData.isNull()) {
                return NULL_ROWS; // for backward compatibility instead of null
            }
            // RawQueryData is always typed
            return () -> rawData.getIterable().iterator();
        }

        if (data instanceof JsonQueryData) {
            JsonQueryData jsonData = (JsonQueryData) data;
            if (jsonData.isNull()) {
                return NULL_ROWS;
            }
            return () -> JsonResultRows.forJsonParser(jsonData.getJsonParser(), columns).iterator();
        }

        if (data instanceof EncodedQueryData) {
            EncodedQueryData encodedData = (EncodedQueryData) data;
            setEncoding(columns, encodedData.getEncoding());
            List<Segment> segments = encodedData.getSegments();

            List<Future<? extends InputStream>> futures = segments.stream().map(segment -> {
                int segmentSize = segment.getSegmentSize();

                if (segment instanceof InlineSegment) {
                    InlineSegment inlineSegment = (InlineSegment) segment;
                    return completedFuture(new ByteArrayInputStream(inlineSegment.getData()));
                }

                if (segment instanceof SpooledSegment) {
                    SpooledSegment spooledSegment = (SpooledSegment) segment;
                    // download segments in parallel
                    return segmentLoaderExecutorService.submit(() -> {
                        lock.lock();
                        try {
                            boolean mustWait = currentSizeInBytes + segmentSize > prefetchBufferSize;
                            if (mustWait) {
                                waitingThreads.addLast(Thread.currentThread());
                            }
                            while (mustWait && waitingThreads.peekFirst() != Thread.currentThread()) {
                                // block if prefetch buffer is full
                                sizeCondition.await();
                                // now unblock the first thread that came in
                            }
                            if (mustWait) {
                                waitingThreads.removeFirst();
                            }
                            currentSizeInBytes += segmentSize;
                        }
                        catch (InterruptedException e) {
                            waitingThreads.remove(Thread.currentThread());
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                        finally {
                            lock.unlock();
                        }
                        // download whole segment
                        InputStream segmentInputStream = loader.load(spooledSegment);
                        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                        byte[] off = new byte[1024];
                        int bytesRead;
                        while ((bytesRead = segmentInputStream.read(off, 0, off.length)) != -1) {
                            buffer.write(off, 0, bytesRead);
                        }
                        buffer.flush();
                        return new ByteArrayInputStream(buffer.toByteArray());
                    });
                }

                throw new UnsupportedOperationException("Unsupported segment type: " + segment.getClass().getName());
            }).collect(toImmutableList());

            List<ResultRows> resultRows = new ArrayList<>();
            forEachPair(futures.stream(), segments.stream(), (future, segment) -> {
                resultRows.add(ResultRows.fromIterableRows(new DeferredIterable(
                        decoderExecutorService.submit(() -> {
                            try {
                                // block decode if segment is not yet downloaded
                                InputStream input = future.get();
                                return decoder.decode(input, segment.getMetadata());
                            }
                            catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                            catch (ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                            catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }),
                        segment.getRowsCount(),
                        new Callable<Void>() {
                            @Override
                            public Void call()
                                    throws Exception
                            {
                                // the data has been read, so we can free up the buffer
                                lock.lock();
                                try {
                                    int segmentSize = segment.getSegmentSize();
                                    currentSizeInBytes -= segmentSize;
                                    sizeCondition.signalAll();
                                }
                                finally {
                                    lock.unlock();
                                }
                                return null;
                            }
                        })));
            });

            return concat(resultRows);
        }

        throw new UnsupportedOperationException("Unsupported data type: " + data.getClass().getName());
    }

    public Optional<String> getEncoding()
    {
        return Optional.ofNullable(decoder)
                .map(QueryDataDecoder::encoding);
    }

    @Override
    public void close()
            throws Exception
    {
        loader.close();
    }

    @SuppressModernizer
    private static ResultRows concat(Iterable<ResultRows> resultRows)
    {
        return fromIterableRows(Iterables.concat(filter(resultRows, rows -> !rows.isNull())));
    }
}
