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
package io.trino.plugin.pulsar;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.impl.ReadOnlyCursorImpl;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;

import java.util.List;

/**
 * A wrapper implementing {@link PulsarReadOnlyCursor} and delegates work to a {@link ReadOnlyCursorImpl}
 */
public class PulsarReadOnlyCursorWrapper
        implements PulsarReadOnlyCursor
{
    ReadOnlyCursor delegate;

    public PulsarReadOnlyCursorWrapper(ReadOnlyCursor readOnlyCursor)
    {
        delegate = readOnlyCursor;
    }

    @Override
    public MLDataFormats.ManagedLedgerInfo.LedgerInfo getCurrentLedgerInfo()
    {
        if (delegate instanceof ReadOnlyCursorImpl) {
            return ((ReadOnlyCursorImpl) delegate).getCurrentLedgerInfo();
        }

        return ((PulsarReadOnlyCursor) delegate).getCurrentLedgerInfo();
    }

    @Override
    public List<Entry> readEntries(int i) throws InterruptedException, ManagedLedgerException
    {
        return delegate.readEntries(i);
    }

    @Override
    public void asyncReadEntries(int i, AsyncCallbacks.ReadEntriesCallback readEntriesCallback, Object o, PositionImpl maxPosition)
    {
        delegate.asyncReadEntries(i, readEntriesCallback, o, maxPosition);
    }

    @Override
    public void asyncReadEntries(int i, long l, AsyncCallbacks.ReadEntriesCallback readEntriesCallback, Object o, PositionImpl maxPosition)
    {
        delegate.asyncReadEntries(i, l, readEntriesCallback, o, maxPosition);
    }

    @Override
    public Position getReadPosition()
    {
        return delegate.getReadPosition();
    }

    @Override
    public boolean hasMoreEntries()
    {
        return delegate.hasMoreEntries();
    }

    @Override
    public long getNumberOfEntries()
    {
        return delegate.getNumberOfEntries();
    }

    @Override
    public void skipEntries(int i)
    {
        delegate.skipEntries(i);
    }

    @Override
    public Position findNewestMatching(
            ManagedCursor.FindPositionConstraint findPositionConstraint,
            org.apache.pulsar.shade.com.google.common.base.Predicate<Entry> predicate)
            throws InterruptedException, ManagedLedgerException
    {
        return delegate.findNewestMatching(findPositionConstraint, predicate);
    }

    @Override
    public long getNumberOfEntries(org.apache.pulsar.shade.com.google.common.collect.Range<PositionImpl> range)
    {
        return delegate.getNumberOfEntries(range);
    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException
    {
        delegate.close();
    }

    @Override
    public void asyncClose(AsyncCallbacks.CloseCallback closeCallback, Object o)
    {
        delegate.asyncClose(closeCallback, o);
    }
}
