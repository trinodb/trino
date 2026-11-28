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
package io.trino.type.collation;

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RawCollationKey;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.type.CollationComparison;

import static java.util.Objects.requireNonNull;

/// A collation implemented by an ICU collator.
///
/// ICU compares Java strings, so each comparison decodes its UTF-8 arguments. A frozen collator
/// is safe for concurrent use, but it serializes comparisons and key computations on an internal
/// lock, so each thread compares with its own unfrozen copy.
final class IcuCollation
        implements CollationComparison
{
    private final ThreadLocal<Collator> threadCollator;

    IcuCollation(Collator collator)
    {
        requireNonNull(collator, "collator is null");
        if (!collator.isFrozen()) {
            throw new IllegalArgumentException("Collator must be frozen");
        }
        this.threadCollator = ThreadLocal.withInitial(collator::cloneAsThawed);
    }

    @Override
    public int compare(Slice left, Slice right)
    {
        return threadCollator.get().compare(left.toStringUtf8(), right.toStringUtf8());
    }

    @Override
    public boolean equal(Slice left, Slice right)
    {
        // identical bytes are equal under every collation; avoid decoding for the common case
        return left.equals(right) || threadCollator.get().compare(left.toStringUtf8(), right.toStringUtf8()) == 0;
    }

    @Override
    public Slice collationKey(Slice value)
    {
        RawCollationKey key = threadCollator.get().getRawCollationKey(value.toStringUtf8(), null);
        return Slices.wrappedBuffer(key.bytes, 0, key.size);
    }
}
