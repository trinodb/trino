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
package io.trino.plugin.pulsar.mock;

import com.google.common.base.Predicate;
import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.trino.plugin.pulsar.PulsarReadOnlyCursor;
import io.trino.plugin.pulsar.TestPulsarConnector;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import static java.lang.Math.toIntExact;

public class MockCursor
        implements PulsarReadOnlyCursor
{
    protected static final long currentTimeMs = 1534806330000L;

    protected Map<String, Integer> positions;

    private Map<String, Long> topicsToNumEntries;

    private Map<String, Function<Integer, Object>> fooFunctions;

    private Map<String, SchemaInfo> topicsToSchemas;

    private LongAdder completedBytes;

    private int count;

    protected String topic;

    private String schemaName;

    private long entries;

    public MockCursor(String topic, String schemaName, long entries,
                      Map<String, Integer> positions, Map<String, Long> topicsToNumEntries,
                      Map<String, Function<Integer, Object>> fooFunctions, Map<String, SchemaInfo> topicsToSchemas,
                      LongAdder completedBytes)
    {
        this.topic = topic;
        this.schemaName = schemaName;
        this.entries = entries;
        this.positions = positions;
        this.topicsToNumEntries = topicsToNumEntries;
        this.fooFunctions = fooFunctions;
        this.topicsToSchemas = topicsToSchemas;
        this.completedBytes = completedBytes;
    }

    @Override
    public long getNumberOfEntries()
    {
        return entries;
    }

    @Override
    public void skipEntries(int numEntriesToSkip)
    {
        positions.put(topic, positions.get(topic) + numEntriesToSkip);
    }

    @Override
    public Position findNewestMatching(ManagedCursor.FindPositionConstraint findPositionConstraint, Predicate<Entry> predicate) throws InterruptedException, ManagedLedgerException
    {
        String schemaName = TopicName.get(
                TopicName.get(
                        topic.replaceAll("/persistent", ""))
                        .getPartitionedTopicName()).getSchemaName();
        List<Entry> entries = getTopicEntries(schemaName);

        Integer target = null;
        for (int i = entries.size() - 1; i >= 0; i--) {
            Entry entry = entries.get(i);
            if (predicate.apply(entry)) {
                target = i;
                break;
            }
        }

        return target == null ? null : new PositionImpl(0, target);
    }

    @Override
    public List<Entry> readEntries(int i) throws InterruptedException, ManagedLedgerException
    {
        return null;
    }

    @Override
    public void asyncReadEntries(int i, AsyncCallbacks.ReadEntriesCallback readEntriesCallback, Object o, PositionImpl maxPosition)
    { }

    @Override
    public void asyncReadEntries(int numberOfEntriesToRead, long maxSizeBytes, AsyncCallbacks.ReadEntriesCallback callback, Object ctx, PositionImpl maxPosition)
    {
        new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                List<Entry> entries = new LinkedList<>();
                for (int i = 0; i < numberOfEntriesToRead; i++) {
                    TestPulsarConnector.Bar bar = new TestPulsarConnector.Bar();
                    bar.field1 = fooFunctions.get("bar.field1").apply(count) == null ? null : (int) fooFunctions.get("bar.field1").apply(count);
                    bar.field2 = fooFunctions.get("bar.field2").apply(count) == null ? null : (String) fooFunctions.get("bar.field2").apply(count);
                    bar.field3 = (float) fooFunctions.get("bar.field3").apply(count);

                    TestPulsarConnector.Foo foo = new TestPulsarConnector.Foo();
                    foo.field1 = (int) fooFunctions.get("field1").apply(count);
                    foo.field2 = (String) fooFunctions.get("field2").apply(count);
                    foo.field3 = (float) fooFunctions.get("field3").apply(count);
                    foo.field4 = (double) fooFunctions.get("field4").apply(count);
                    foo.field5 = (boolean) fooFunctions.get("field5").apply(count);
                    foo.field6 = (long) fooFunctions.get("field6").apply(count);
                    foo.timestamp = (long) fooFunctions.get("timestamp").apply(count);
                    foo.time = (int) fooFunctions.get("time").apply(count);
                    foo.date = (int) fooFunctions.get("date").apply(count);
                    foo.bar = bar;
                    foo.field7 = (TestPulsarConnector.Foo.TestEnum) fooFunctions.get("field7").apply(count);

                    MessageMetadata messageMetadata =
                            new MessageMetadata()
                            .setProducerName("test-producer").setSequenceId(positions.get(topic))
                            .setPublishTime(System.currentTimeMillis());

                    Schema schema = topicsToSchemas.get(schemaName).getType() == SchemaType.AVRO ? AvroSchema.of(TestPulsarConnector.Foo.class) : JSONSchema.of(TestPulsarConnector.Foo.class);

                    ByteBuf payload = Unpooled.copiedBuffer(schema.encode(foo));

                    ByteBuf byteBuf = org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload(
                            org.apache.pulsar.common.protocol.Commands.ChecksumType.Crc32c, messageMetadata, payload);

                    completedBytes.add(byteBuf.readableBytes());

                    entries.add(EntryImpl.create(0, positions.get(topic), byteBuf));
                    positions.put(topic, positions.get(topic) + 1);
                    count++;
                }

                callback.readEntriesComplete(entries, ctx);
            }
        }).start();
    }

    @Override
    public Position getReadPosition()
    {
        return PositionImpl.get(0, positions.get(topic));
    }

    @Override
    public boolean hasMoreEntries()
    {
        return positions.get(topic) < entries;
    }

    @Override
    public long getNumberOfEntries(Range<PositionImpl> range)
    {
        return (range.upperEndpoint().getEntryId() + 1) - range.lowerEndpoint().getEntryId();
    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException
    { }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback closeCallback, Object o)
    { }

    @Override
    public MLDataFormats.ManagedLedgerInfo.LedgerInfo getCurrentLedgerInfo()
    {
        return MLDataFormats.ManagedLedgerInfo.LedgerInfo.newBuilder().setLedgerId(0).build();
    }

    public List<Entry> getTopicEntries(String topicSchemaName)
    {
        List<Entry> entries = new LinkedList<>();

        long count = topicsToNumEntries.get(topicSchemaName);
        for (int i = 0; i < count; i++) {
            TestPulsarConnector.Foo foo = new TestPulsarConnector.Foo();
            foo.field1 = (int) count;
            foo.field2 = String.valueOf(count);
            foo.field3 = count;
            foo.field4 = count;
            foo.field5 = count % 2 == 0;
            foo.field6 = count;
            foo.timestamp = System.currentTimeMillis();

            LocalTime now = LocalTime.now(ZoneId.systemDefault());
            foo.time = now.toSecondOfDay() * 1000;

            LocalDate localDate = LocalDate.now();
            LocalDate epoch = LocalDate.ofEpochDay(0);
            foo.date = toIntExact(ChronoUnit.DAYS.between(epoch, localDate));

            MessageMetadata messageMetadata = new MessageMetadata()
                    .setProducerName("test-producer").setSequenceId(i)
                    .setPublishTime(currentTimeMs + i);

            Schema schema = topicsToSchemas.get(topicSchemaName).getType() == SchemaType.AVRO ? AvroSchema.of(SchemaDefinition.<TestPulsarConnector.Foo>builder().withPojo(TestPulsarConnector.Foo.class).build()) : JSONSchema.of(SchemaDefinition.<TestPulsarConnector.Foo>builder().withPojo(TestPulsarConnector.Foo.class).build());

            ByteBuf dataPayload = Unpooled
                    .wrappedBuffer(schema.encode(foo));

            ByteBuf byteBuf = org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload(
                    org.apache.pulsar.common.protocol.Commands.ChecksumType.Crc32c, messageMetadata, dataPayload);

            Entry entry = EntryImpl.create(0, i, byteBuf);
            entries.add(entry);
        }
        return entries;
    }
}
