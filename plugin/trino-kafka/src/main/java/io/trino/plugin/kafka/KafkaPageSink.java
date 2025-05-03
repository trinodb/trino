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
package io.trino.plugin.kafka;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.plugin.kafka.encoder.RowEncoder;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorSession;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.kafka.KafkaErrorCode.KAFKA_PRODUCER_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class KafkaPageSink
        implements ConnectorPageSink
{
    private final String topicName;
    private final List<KafkaColumnHandle> columns;
    private final RowEncoder keyEncoder;
    private final RowEncoder messageEncoder;
    private final KafkaProducer<byte[], byte[]> producer;
    private final ProducerCallback producerCallback;
    private long expectedWrittenBytes;

    public KafkaPageSink(
            String topicName,
            List<KafkaColumnHandle> columns,
            RowEncoder keyEncoder,
            RowEncoder messageEncoder,
            KafkaProducerFactory producerFactory,
            ConnectorSession session)
    {
        this.topicName = requireNonNull(topicName, "topicName is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        this.keyEncoder = requireNonNull(keyEncoder, "keyEncoder is null");
        this.messageEncoder = requireNonNull(messageEncoder, "messageEncoder is null");
        requireNonNull(producerFactory, "producerFactory is null");
        requireNonNull(session, "session is null");
        this.producer = producerFactory.create(session);
        this.producerCallback = new ProducerCallback();
        this.expectedWrittenBytes = 0;
    }

    private static class ProducerCallback
            implements Callback
    {
        private long errorCount;
        private long writtenBytes;

        public ProducerCallback()
        {
            this.errorCount = 0;
            this.writtenBytes = 0;
        }

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e)
        {
            if (e != null) {
                errorCount++;
            }
            else {
                writtenBytes += recordMetadata.serializedValueSize() + recordMetadata.serializedKeySize();
            }
        }

        public long getErrorCount()
        {
            return errorCount;
        }

        public long getWrittenBytes()
        {
            return writtenBytes;
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return producerCallback.getWrittenBytes();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        byte[] keyBytes;
        byte[] messageBytes;

        for (int position = 0; position < page.getPositionCount(); position++) {
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                if (columns.get(channel).isKeyCodec()) {
                    keyEncoder.appendColumnValue(page.getBlock(channel), position);
                }
                else {
                    messageEncoder.appendColumnValue(page.getBlock(channel), position);
                }
            }

            keyBytes = keyEncoder.toByteArray();
            messageBytes = messageEncoder.toByteArray();

            expectedWrittenBytes += keyBytes.length + messageBytes.length;

            producer.send(new ProducerRecord<>(topicName, keyBytes, messageBytes), producerCallback);
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        producer.flush();
        producer.close();
        try {
            keyEncoder.close();
            messageEncoder.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to close row encoders", e);
        }

        checkArgument(
                producerCallback.getWrittenBytes() == expectedWrittenBytes,
                "Actual written bytes: '%s' not equal to expected written bytes: '%s'",
                producerCallback.getWrittenBytes(),
                expectedWrittenBytes);

        if (producerCallback.getErrorCount() > 0) {
            throw new TrinoException(KAFKA_PRODUCER_ERROR, format("%d producer record(s) failed to send", producerCallback.getErrorCount()));
        }

        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        producer.close();
    }
}
