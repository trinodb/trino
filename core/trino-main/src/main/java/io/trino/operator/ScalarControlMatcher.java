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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Matches eight control slots at a time using SWAR arithmetic on a single long.
 * The match set holds {@code 0x80} in the byte of every matching slot.
 * <p>
 * A borrow out of a matching byte can turn the byte above it into a spurious match, which is why
 * {@link ControlMatcher#match} only promises a superset of the matching occupied slots. An empty
 * slot is never reported spuriously, because a borrow cannot clear the high bit that every occupied
 * control byte carries.
 */
final class ScalarControlMatcher
        implements ControlMatcher
{
    private static final int VECTOR_LENGTH = Long.BYTES;
    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);

    @Override
    public int vectorLength()
    {
        return VECTOR_LENGTH;
    }

    @Override
    public long match(byte[] control, int position, byte hashPrefix)
    {
        long vector = (long) LONG_HANDLE.get(control, position);
        long repeated = (hashPrefix & 0xFFL) * 0x01_01_01_01_01_01_01_01L;
        // HD 6-1
        long comparison = vector ^ repeated;
        return (comparison - 0x01_01_01_01_01_01_01_01L) & ~comparison & 0x80_80_80_80_80_80_80_80L;
    }

    @Override
    public int firstSlot(long matches)
    {
        return Long.numberOfTrailingZeros(matches) >>> 3;
    }
}
