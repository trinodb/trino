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
package io.trino.spi.block.vstream;

public final class BitMath
{
    private BitMath() {}

    // Ensures that int will be always unsigned
    public static int zigZag(int value)
    {
        return (value >> 31) ^ (value << 1);
    }

    // Reverses zigZag ;-)
    public static int zagZig(int value)
    {
        return ((value >>> 1) ^ -(value & 1));
    }

    // Ensures that long will be always unsigned
    public static long zigZag(long value)
    {
        return (value >> 63) ^ (value << 1);
    }

    // Reverses zigZag ;-)
    public static long zagZig(long value)
    {
        return ((value >>> 1) ^ -(value & 1));
    }
}
