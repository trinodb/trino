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
package io.trino.spi;

import static java.lang.Long.rotateLeft;

public class HashUtils
{
    // Constants from xxHash64
    private static final long PRIME64_1 = 0x9E3779B185EBCA87L;
    private static final long PRIME64_2 = 0xC2B2AE3D27D4EB4FL;

    private HashUtils() {}

    public static long hash(long value)
    {
        return mix(0, value);
    }

    /**
     * Mix function from xxHash64.
     * May be used to cheaply mix two hashes together.
     */
    public static long mix(long value1, long value2)
    {
        return rotateLeft(value1 + value2 * PRIME64_2, 31) * PRIME64_1;
    }
}
