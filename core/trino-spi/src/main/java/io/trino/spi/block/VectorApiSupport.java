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
package io.trino.spi.block;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.ShortVector;
import jdk.incubator.vector.VectorSpecies;

/// Sole supplier of vector species for the block encodings. Obtaining them here instead of directly
/// from the Vector API turns the `NoClassDefFoundError` thrown on a JVM without the
/// `jdk.incubator.vector` module into an actionable error message.
final class VectorApiSupport
{
    private VectorApiSupport() {}

    static VectorSpecies<Byte> preferredByteSpecies()
    {
        verifyEnabled();
        return ByteVector.SPECIES_PREFERRED;
    }

    static VectorSpecies<Short> preferredShortSpecies()
    {
        verifyEnabled();
        return ShortVector.SPECIES_PREFERRED;
    }

    static VectorSpecies<Integer> preferredIntSpecies()
    {
        verifyEnabled();
        return IntVector.SPECIES_PREFERRED;
    }

    static VectorSpecies<Long> preferredLongSpecies()
    {
        verifyEnabled();
        return LongVector.SPECIES_PREFERRED;
    }

    private static void verifyEnabled()
    {
        if (ModuleLayer.boot().findModule("jdk.incubator.vector").isEmpty()) {
            throw new IllegalStateException("The Java Vector API is not enabled for this JVM. Add --add-modules=jdk.incubator.vector to the JVM options");
        }
    }
}
