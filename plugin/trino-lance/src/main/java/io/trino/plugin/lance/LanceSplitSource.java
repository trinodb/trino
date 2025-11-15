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
package io.trino.plugin.lance;

import io.trino.plugin.lance.metadata.Fragment;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class LanceSplitSource
        implements ConnectorSplitSource
{
    public static final long MAX_ROWS_PER_SPLIT = 97L;

    private final SplitIterator splitIterator;

    public LanceSplitSource(List<Fragment> fragments)
    {
        requireNonNull(fragments, "fragments is null");
        this.splitIterator = new SplitIterator(fragments);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        List<ConnectorSplit> splits = new ArrayList<>(maxSize);
        while (splits.size() < maxSize && splitIterator.hasNext()) {
            splits.add(splitIterator.next());
        }
        return completedFuture(new ConnectorSplitBatch(splits, isFinished()));
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean isFinished()
    {
        return !splitIterator.hasNext();
    }

    public static class SplitIterator
            implements Iterator<LanceSplit>
    {
        private final List<Fragment> fragments;

        private int fragmentIndex;
        private long offsetInFragment;

        public SplitIterator(List<Fragment> fragments)
        {
            this.fragments = requireNonNull(fragments, "fragments is null");
        }

        @Override
        public boolean hasNext()
        {
            return fragmentIndex < fragments.size();
        }

        @Override
        public LanceSplit next()
        {
            if (!hasNext()) {
                return null;
            }
            Fragment fragment = fragments.get(fragmentIndex);
            long rowsLeft = fragment.physicalRows() - offsetInFragment;
            long length = Math.min(rowsLeft, MAX_ROWS_PER_SPLIT);

            long start = offsetInFragment;
            long end = offsetInFragment + length; // exclusive

            offsetInFragment += length;
            if (offsetInFragment >= fragment.physicalRows()) {
                fragmentIndex++;
                offsetInFragment = 0;
            }

            return new LanceSplit(fragment, start, end);
        }
    }
}
