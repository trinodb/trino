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

import io.airlift.slice.Slice;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
final class SerializedPageReference
{
    private static final AtomicIntegerFieldUpdater<SerializedPageReference> REFERENCE_COUNT_UPDATER = AtomicIntegerFieldUpdater.newUpdater(SerializedPageReference.class, "referenceCount");

    private final Slice serializedPage;
    private final int positionCount;
    private volatile int referenceCount;

    public SerializedPageReference(Slice serializedPage, int positionCount, int referenceCount)
    {
        this.serializedPage = requireNonNull(serializedPage, "serializedPage is null");
        checkArgument(referenceCount > 0, "referenceCount must be at least 1");
        this.positionCount = positionCount;
        this.referenceCount = referenceCount;
    }

    public void addReference()
    {
        int oldReferences = REFERENCE_COUNT_UPDATER.getAndIncrement(this);
        checkState(oldReferences > 0, "Page has already been dereferenced");
    }

    public Slice getSerializedPage()
    {
        return serializedPage;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public long getRetainedSizeInBytes()
    {
        return serializedPage.getRetainedSize();
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

    public static void dereferencePages(List<SerializedPageReference> serializedPageReferences, PagesReleasedListener onPagesReleased)
    {
        requireNonNull(serializedPageReferences, "serializedPageReferences is null");
        requireNonNull(onPagesReleased, "onPagesReleased is null");
        int releasedPageCount = 0;
        long releasedMemorySizeInBytes = 0;
        for (SerializedPageReference serializedPageReference : serializedPageReferences) {
            if (serializedPageReference.dereferencePage()) {
                releasedPageCount++;
                releasedMemorySizeInBytes += serializedPageReference.getRetainedSizeInBytes();
            }
        }
        if (releasedPageCount > 0) {
            onPagesReleased.onPagesReleased(releasedPageCount, releasedMemorySizeInBytes);
        }
    }

    interface PagesReleasedListener
    {
        void onPagesReleased(int releasedPagesCount, long releasedMemorySizeInBytes);

        static PagesReleasedListener forOutputBufferMemoryManager(OutputBufferMemoryManager memoryManager)
        {
            return (releasedPagesCount, releasedMemorySizeInBytes) -> memoryManager.updateMemoryUsage(-releasedMemorySizeInBytes);
        }
    }
}
