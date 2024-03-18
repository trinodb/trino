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
package io.trino.plugin.varada.util;

import java.nio.ByteBuffer;
import java.util.Arrays;

public record StringPredicateData(
        ByteBuffer value,
        int length,
        long comperationValue)
        implements Comparable<StringPredicateData>
{
    @Override
    public int compareTo(StringPredicateData o)
    {
        return Long.compare(comperationValue, o.comperationValue());
    }

    @Override
    public ByteBuffer value()
    {
        value.position(0);
        return value;
    }

    @Override
    public String toString()
    {
        return "StringCrc{" +
                "value=" + (value.hasArray() ? Arrays.toString(Arrays.copyOfRange(value.array(), value.arrayOffset(), value.remaining())) : "<buffer without an array>") +
                ", comperationValue=" + Long.toHexString(comperationValue) +
                '}';
    }
}
