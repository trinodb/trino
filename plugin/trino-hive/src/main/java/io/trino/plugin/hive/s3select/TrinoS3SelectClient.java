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
package io.trino.plugin.hive.s3select;

import io.airlift.log.Logger;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.EndEvent;
import software.amazon.awssdk.services.s3.model.RecordsEvent;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayDeque;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.Objects.requireNonNull;

class TrinoS3SelectClient
        implements Closeable
{
    private static final Logger log = Logger.get("TrinoS3SelectClient");
    private final S3AsyncClient s3Client;
    private boolean requestComplete;
    private SelectObjectContentRequest selectObjectRequest;

    public TrinoS3SelectClient(Configuration configuration, TrinoS3ClientFactory s3ClientFactory)
    {
        requireNonNull(configuration, "configuration is null");
        requireNonNull(s3ClientFactory, "s3ClientFactory is null");
        this.s3Client = s3ClientFactory.getS3Client(configuration);
    }

    public InputStream getRecordsContent(SelectObjectContentRequest selectObjectRequest)
    {
        this.selectObjectRequest = requireNonNull(selectObjectRequest, "selectObjectRequest is null");
        EventStreamEnumeration eventStreamEnumeration = new EventStreamEnumeration();
        InputStream recordInputStream = new SequenceInputStream(eventStreamEnumeration);

        SelectObjectContentResponseHandler.Visitor visitor = new SelectObjectContentResponseHandler.Visitor()
        {
            @Override
            public void visitRecords(RecordsEvent event)
            {
                eventStreamEnumeration.addEvent(new S3Event(event, false));
            }

            @Override
            public void visitEnd(EndEvent event)
            {
                requestComplete = true;
                eventStreamEnumeration.addEvent(new S3Event(null, true));
            }
        };

        s3Client.selectObjectContent(selectObjectRequest, SelectObjectContentResponseHandler.builder().subscriber(visitor).build()).join();
        return recordInputStream;
    }

    @Override
    public void close()
            throws IOException
    {
       // No-Op
    }

    public String getKeyName()
    {
        return selectObjectRequest.key();
    }

    public String getBucketName()
    {
        return selectObjectRequest.bucket();
    }

    /**
     * The End Event indicates all matching records have been transmitted.
     * If the End Event is not received, the results may be incomplete.
     */
    public boolean isRequestComplete()
    {
        return requestComplete;
    }

    /**
     * Below classes are required for compatibility between AWS Java SDK 1.x and 2.x
     * They return an InputStream to all the incoming record events
     */
    static class S3Event
    {
        RecordsEvent event;
        boolean isEndEvent;

        public S3Event(RecordsEvent event, boolean isEndEvent)
        {
            this.event = event;
            this.isEndEvent = isEndEvent;
        }
    }

    private static class EventStreamEnumeration
            extends LazyLoadedIterator<InputStream> implements Enumeration<InputStream>
    {
        private boolean initialized;
        private final BlockingQueue<S3Event> inputStreams;

        EventStreamEnumeration()
        {
            this.inputStreams = new LinkedBlockingQueue<>();
        }

        @Override
        protected Optional<? extends InputStream> getNext()
                throws InterruptedException
        {
            if (!initialized) {
                initialized = true;
                return Optional.of(new ByteArrayInputStream(new byte[0]));
            }

            S3Event s3Event = inputStreams.take();
            if (s3Event.isEndEvent) {
                return Optional.empty();
            }
            return Optional.of(s3Event.event.payload().asInputStream());
        }

        public void addEvent(S3Event event)
        {
            this.inputStreams.add(event);
        }

        @Override
        public boolean hasMoreElements()
        {
            return super.hasNext();
        }

        @Override
        public InputStream nextElement()
        {
            return super.next();
        }
    }

    private abstract static class LazyLoadedIterator<T>
            implements Iterator<T>
    {
        private final Queue<T> next = new ArrayDeque<T>();
        private boolean isDone;

        @Override
        public boolean hasNext()
        {
            advanceIfNeeded();
            return !isDone;
        }

        @Override
        public T next()
        {
            advanceIfNeeded();

            if (isDone) {
                throw new NoSuchElementException();
            }

            return next.poll();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        private void advanceIfNeeded()
        {
            if (!isDone && next.isEmpty()) {
                try {
                    Optional<? extends T> nextElement = getNext();
                    nextElement.ifPresent(this.next::add);
                    this.isDone = this.next.isEmpty();
                }
                catch (InterruptedException e) {
                    log.error("Failed to read during S3 Select : %s", e.getMessage());
                }
            }
        }

        /**
         * Load any newly-available events. This can return any number of events, in the order they should be encountered by the
         * user of the iterator. This should return an empty collection if there are no remaining events in the stream.
         */
        protected abstract Optional<? extends T> getNext()
                throws InterruptedException;
    }
}
