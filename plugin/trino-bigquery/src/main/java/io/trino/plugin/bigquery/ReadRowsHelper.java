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
package io.trino.plugin.bigquery;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import io.airlift.log.Logger;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

public class ReadRowsHelper
{
    private static final Logger log = Logger.get(ReadRowsHelper.class);

    private final BigQueryReadClient client;
    private final String streamName;
    private final int maxReadRowsRetries;

    public ReadRowsHelper(BigQueryReadClient client, String streamName, int maxReadRowsRetries)
    {
        this.client = requireNonNull(client, "client cannot be null");
        this.streamName = requireNonNull(streamName, "streamName cannot be null");
        this.maxReadRowsRetries = maxReadRowsRetries;
    }

    ReadRowsIterator readRows()
    {
        return new ReadRowsIterator(this, fetchResponses(0));
    }

    // In order to enable testing
    protected ServerStream<ReadRowsResponse> fetchResponses(long offset)
    {
        log.debug("Reading rows from %s offset %s", streamName, offset);
        return client.readRowsCallable()
                .call(ReadRowsRequest.newBuilder()
                        .setReadStream(streamName)
                        .setOffset(offset)
                        .build());
    }

    // Ported from https://github.com/GoogleCloudDataproc/spark-bigquery-connector/pull/150
    static final class ReadRowsIterator
            implements Iterator<ReadRowsResponse>, Closeable
    {
        private final ReadRowsHelper helper;
        private long nextOffset;
        private volatile ServerStream<ReadRowsResponse> serverStream;
        private Iterator<ReadRowsResponse> serverResponses;
        private int retries;
        private volatile boolean closed;

        public ReadRowsIterator(
                ReadRowsHelper helper,
                ServerStream<ReadRowsResponse> serverStream)
        {
            this.helper = helper;
            this.serverStream = serverStream;
            this.serverResponses = serverStream.iterator();
        }

        @Override
        public boolean hasNext()
        {
            return serverResponses.hasNext();
        }

        @Override
        public ReadRowsResponse next()
        {
            do {
                try {
                    ReadRowsResponse response = serverResponses.next();
                    log.debug("ReadRowsResponse from BigQuery stream: %s at offset: %s", helper.streamName, nextOffset);
                    nextOffset += response.getRowCount();
                    return response;
                }
                catch (Exception e) {
                    // if relevant, retry the read, from the last read position
                    if (BigQueryUtil.isRetryable(e) && retries < helper.maxReadRowsRetries) {
                        log.debug("Request failed, retrying: %s", e);
                        serverStream = helper.fetchResponses(nextOffset);
                        serverResponses = serverStream.iterator();
                        if (closed) {
                            serverStream.cancel();
                        }
                        retries++;
                    }
                    else {
                        helper.client.close();
                        throw e;
                    }
                }
            }
            while (serverResponses.hasNext());

            throw new NoSuchElementException("No more server responses");
        }

        @Override
        public void close()
        {
            closed = true;
            // Closing the client does not cancel in-flight calls, which keep holding read connection quota
            serverStream.cancel();
        }
    }
}
