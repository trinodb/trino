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

/**
 * Generic SIMD capability interface exposed by SPI.
 * Implementations should answer true only when the operation is supported
 * without emulation on the current CPU/JVM.
 */
public interface SimdSupport
{
    default boolean supportByteGeneric()
    {
        return false;
    }

    default boolean supportShortGeneric()
    {
        return false;
    }

    default boolean supportIntegerGeneric()
    {
        return false;
    }

    default boolean supportLongGeneric()
    {
        return false;
    }

    default boolean supportByteCompress()
    {
        return false;
    }

    default boolean supportShortCompress()
    {
        return false;
    }

    default boolean supportIntegerCompress()
    {
        return false;
    }

    default boolean supportLongCompress()
    {
        return false;
    }

    SimdSupport NONE = new SimdSupport() {};
}
