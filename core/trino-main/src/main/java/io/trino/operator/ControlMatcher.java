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
package io.trino.operator;

/**
 * Searches the control bytes of a flat hash table {@link #vectorLength()} slots at a time.
 * <p>
 * A control byte is either zero, meaning the slot is empty, or the seven low bits of the hash
 * with the high bit set. The table stores {@link #vectorLength()} extra control bytes mirroring
 * the first slots, so a vector can always be read at any slot without wrapping.
 */
sealed interface ControlMatcher
        permits ScalarControlMatcher, VectorControlMatcher
{
    /**
     * The fastest matcher for the hardware this server runs on. This must not be a field of this
     * interface, as initializing an implementation initializes this interface first, which would
     * then observe the implementation as half initialized.
     */
    static ControlMatcher preferred()
    {
        return VectorControlMatcher.isSupported() ? new VectorControlMatcher() : new ScalarControlMatcher();
    }

    /**
     * Number of consecutive control slots inspected by a single {@link #match} call.
     * Always a power of two.
     */
    int vectorLength();

    /**
     * Finds the slots matching {@code hashPrefix} within the {@link #vectorLength()} slots starting
     * at {@code position}. The result is an opaque match set which must be inspected with
     * {@link #firstSlot(long)} and {@link #clearFirstSlot(long)}, and is zero when there is no match.
     * <p>
     * A match set never misses a matching slot, but when matching a hash prefix, it may contain
     * occupied slots that do not actually match, so the caller must compare the values of the
     * matched slots anyway. Matching the empty control byte is exact, as an empty slot is never
     * reported as occupied and vice versa.
     */
    long match(byte[] control, int position, byte hashPrefix);

    /**
     * Slot offset, relative to the start of the vector, of the first match in the match set.
     * The match set must not be empty.
     */
    int firstSlot(long matches);

    /**
     * Removes the first match from the match set.
     */
    default long clearFirstSlot(long matches)
    {
        return matches & (matches - 1);
    }
}
