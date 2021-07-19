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

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Supplier;

public class MockManagedLedgerFactory
        implements ManagedLedgerFactory
{
    protected Map<String, Integer> positions = new HashMap<>();

    private Map<String, Long> topicsToNumEntries;

    private Map<String, Function<Integer, Object>> fooFunctions;

    private Map<String, SchemaInfo> topicsToSchemas;

    private LongAdder completedBytes;

    public MockManagedLedgerFactory(Map<String, Long> topicsToNumEntries, Map<String, Function<Integer, Object>> fooFunctions,
                                    Map<String, SchemaInfo> topicsToSchemas, LongAdder completedBytes)
    {
        this.topicsToNumEntries = topicsToNumEntries;
        this.fooFunctions = fooFunctions;
        this.topicsToSchemas = topicsToSchemas;
        this.completedBytes = completedBytes;
    }

    @Override
    public ManagedLedger open(String s)
    {
        return null;
    }

    @Override
    public ManagedLedger open(String s, ManagedLedgerConfig managedLedgerConfig)
    {
        return null;
    }

    @Override
    public void asyncOpen(String s, AsyncCallbacks.OpenLedgerCallback openLedgerCallback, Object o)
    { }

    @Override
    public void asyncOpen(String s, ManagedLedgerConfig managedLedgerConfig, AsyncCallbacks.OpenLedgerCallback openLedgerCallback, Supplier<Boolean> supplier, Object o)
    { }

    @Override
    public ReadOnlyCursor openReadOnlyCursor(String topic, Position position, ManagedLedgerConfig managedLedgerConfig) throws InterruptedException, ManagedLedgerException
    {
        PositionImpl positionImpl = (PositionImpl) position;

        int entryId = positionImpl.getEntryId() == -1 ? 0 : (int) positionImpl.getEntryId();

        positions.put(topic, entryId);
        String schemaName = TopicName.get(
                TopicName.get(
                        topic.replaceAll("/persistent", ""))
                        .getPartitionedTopicName()).getSchemaName();
        return new MockCursor(topic, schemaName, topicsToNumEntries.get(schemaName),
                positions, topicsToNumEntries, fooFunctions, topicsToSchemas, completedBytes);
    }

    @Override
    public void asyncOpenReadOnlyCursor(String s, Position position, ManagedLedgerConfig managedLedgerConfig, AsyncCallbacks.OpenReadOnlyCursorCallback openReadOnlyCursorCallback, Object o)
    { }

    @Override
    public ManagedLedgerInfo getManagedLedgerInfo(String s) throws InterruptedException, ManagedLedgerException
    {
        return null;
    }

    @Override
    public void asyncGetManagedLedgerInfo(String s, AsyncCallbacks.ManagedLedgerInfoCallback managedLedgerInfoCallback, Object o)
    { }

    @Override
    public void delete(String s) throws InterruptedException, ManagedLedgerException
    { }

    @Override
    public void asyncDelete(String s, AsyncCallbacks.DeleteLedgerCallback deleteLedgerCallback, Object o)
    { }

    @Override
    public void shutdown()
    { }
}
