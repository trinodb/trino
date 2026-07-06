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
import io.trino.spi.Page;
import jakarta.annotation.Nullable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * A page received from an exchange, either in serialized form, or by reference
 * when the producer task runs on the same node.
 */
public final class ExchangedPage
{
    @Nullable
    private final Slice serializedPage;
    @Nullable
    private final Page page;

    private ExchangedPage(@Nullable Slice serializedPage, @Nullable Page page)
    {
        this.serializedPage = serializedPage;
        this.page = page;
    }

    public static ExchangedPage serialized(Slice serializedPage)
    {
        return new ExchangedPage(requireNonNull(serializedPage, "serializedPage is null"), null);
    }

    public static ExchangedPage raw(Page page)
    {
        return new ExchangedPage(null, requireNonNull(page, "page is null"));
    }

    /**
     * Returns the received page, deserializing it if it traveled in serialized form.
     * A page received by reference from a task running on this node is returned as is.
     */
    public Page page(PageDeserializer deserializer)
    {
        requireNonNull(deserializer, "deserializer is null");
        if (page != null) {
            return page;
        }
        return deserializer.deserialize(serializedPage);
    }

    public boolean isSerialized()
    {
        return serializedPage != null;
    }

    public Slice serializedPage()
    {
        checkState(serializedPage != null, "page is not serialized");
        return serializedPage;
    }

    public Page rawPage()
    {
        checkState(page != null, "page is serialized");
        return page;
    }

    /**
     * The size of the page as received: the serialized size, or the logical size
     * of a page received by reference.
     */
    public long sizeInBytes()
    {
        if (serializedPage != null) {
            return serializedPage.length();
        }
        return page.getSizeInBytes();
    }

    public long retainedSizeInBytes()
    {
        if (serializedPage != null) {
            return serializedPage.getRetainedSize();
        }
        return page.getRetainedSizeInBytes();
    }

    /**
     * The number of bytes that actually traveled over the network: the serialized size,
     * or zero for a page received by reference from a task on the same node.
     */
    public long networkSizeInBytes()
    {
        if (serializedPage != null) {
            return serializedPage.length();
        }
        return 0;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ExchangedPage that)) {
            return false;
        }
        return Objects.equals(serializedPage, that.serializedPage) && Objects.equals(page, that.page);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(serializedPage, page);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("serializedPage", serializedPage)
                .add("page", page)
                .toString();
    }
}
