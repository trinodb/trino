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
package io.trino.execution.buffer;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.ThreadSafe;
import io.airlift.slice.Slice;
import io.trino.spi.Page;

import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.execution.buffer.PagesSerdeUtil.getSerializedPagePositionCount;
import static java.util.Objects.requireNonNull;

/**
 * A reference counted page buffered in a task output buffer, either in serialized form
 * or as a raw {@link Page} when the consumer runs on the same node.
 */
@ThreadSafe
final class SerializedPageReference
{
    private static final AtomicIntegerFieldUpdater<SerializedPageReference> REFERENCE_COUNT_UPDATER = AtomicIntegerFieldUpdater.newUpdater(SerializedPageReference.class, "referenceCount");

    private final ExchangedPage page;
    private final int positionCount;
    private volatile int referenceCount;

    public SerializedPageReference(Slice serializedPage, int positionCount, int referenceCount)
    {
        this(ExchangedPage.serialized(serializedPage), positionCount, referenceCount);
    }

    public SerializedPageReference(Page rawPage, int referenceCount)
    {
        this(ExchangedPage.raw(rawPage), rawPage.getPositionCount(), referenceCount);
    }

    private SerializedPageReference(ExchangedPage page, int positionCount, int referenceCount)
    {
        this.page = requireNonNull(page, "page is null");
        checkArgument(referenceCount > 0, "referenceCount must be at least 1");
        this.positionCount = positionCount;
        this.referenceCount = referenceCount;
    }

    public void addReference()
    {
        int oldReferences = REFERENCE_COUNT_UPDATER.getAndIncrement(this);
        checkState(oldReferences > 0, "Page has already been dereferenced");
    }

    public boolean isSerialized()
    {
        return page.isSerialized();
    }

    public Slice getSerializedPage()
    {
        return page.serializedPage();
    }

    public Page getRawPage()
    {
        return page.rawPage();
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public long getRetainedSizeInBytes()
    {
        return page.retainedSizeInBytes();
    }

    /**
     * The size of the buffered data: the serialized length, or the logical size of a raw
     * page. Used for flow control, where the retained size would penalize raw pages for
     * memory over-allocated by the producing operators.
     */
    public long getSizeInBytes()
    {
        return page.sizeInBytes();
    }

    private boolean dereferencePage()
    {
        int remainingReferences = REFERENCE_COUNT_UPDATER.decrementAndGet(this);
        checkState(remainingReferences >= 0, "Page reference count is negative");
        return remainingReferences == 0;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("referenceCount", referenceCount)
                .toString();
    }

    /**
     * Wraps pages into references with a single initial reference, computing the totals
     * needed to account for them in the output buffer.
     */
    public static PageReferences referenceSerializedPages(List<Slice> pages)
    {
        ImmutableList.Builder<SerializedPageReference> references = ImmutableList.builderWithExpectedSize(pages.size());
        long retainedSizeInBytes = 0;
        long sizeInBytes = 0;
        long rowCount = 0;
        for (Slice page : pages) {
            retainedSizeInBytes += page.getRetainedSize();
            sizeInBytes += page.length();
            int positionCount = getSerializedPagePositionCount(page);
            rowCount += positionCount;
            references.add(new SerializedPageReference(page, positionCount, 1));
        }
        return new PageReferences(references.build(), retainedSizeInBytes, sizeInBytes, rowCount);
    }

    /**
     * Wraps raw pages passed by reference from the producing operator, see
     * {@link #referenceSerializedPages(List)}.
     */
    public static PageReferences referenceRawPages(List<Page> pages)
    {
        ImmutableList.Builder<SerializedPageReference> references = ImmutableList.builderWithExpectedSize(pages.size());
        long retainedSizeInBytes = 0;
        long sizeInBytes = 0;
        long rowCount = 0;
        for (Page page : pages) {
            retainedSizeInBytes += page.getRetainedSizeInBytes();
            sizeInBytes += page.getSizeInBytes();
            rowCount += page.getPositionCount();
            references.add(new SerializedPageReference(page, 1));
        }
        return new PageReferences(references.build(), retainedSizeInBytes, sizeInBytes, rowCount);
    }

    record PageReferences(List<SerializedPageReference> references, long retainedSizeInBytes, long sizeInBytes, long rowCount)
    {
        PageReferences
        {
            references = ImmutableList.copyOf(references);
        }
    }

    public static void dereferencePages(List<SerializedPageReference> serializedPageReferences, PagesReleasedListener onPagesReleased)
    {
        requireNonNull(serializedPageReferences, "serializedPageReferences is null");
        requireNonNull(onPagesReleased, "onPagesReleased is null");
        int releasedPageCount = 0;
        long releasedRetainedSizeInBytes = 0;
        long releasedSizeInBytes = 0;
        for (SerializedPageReference serializedPageReference : serializedPageReferences) {
            if (serializedPageReference.dereferencePage()) {
                releasedPageCount++;
                releasedRetainedSizeInBytes += serializedPageReference.getRetainedSizeInBytes();
                releasedSizeInBytes += serializedPageReference.getSizeInBytes();
            }
        }
        if (releasedPageCount > 0) {
            onPagesReleased.onPagesReleased(releasedPageCount, releasedRetainedSizeInBytes, releasedSizeInBytes);
        }
    }

    interface PagesReleasedListener
    {
        void onPagesReleased(int releasedPagesCount, long releasedRetainedSizeInBytes, long releasedSizeInBytes);

        static PagesReleasedListener forOutputBufferMemoryManager(OutputBufferMemoryManager memoryManager)
        {
            return (_, releasedRetainedSizeInBytes, releasedSizeInBytes) -> memoryManager.updateMemoryUsage(-releasedRetainedSizeInBytes, -releasedSizeInBytes);
        }
    }
}
