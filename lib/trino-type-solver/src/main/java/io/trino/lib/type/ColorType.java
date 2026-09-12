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
package io.trino.lib.type;

import org.weakref.solver.type.PrimitiveTypeConstructor;
import org.weakref.solver.type.Type;
import org.weakref.solver.type.TypeConstructor;
import org.weakref.solver.type.TypeConstructor.Trait;

public record ColorType()
        implements Type
{
    // Comparable but not orderable, mirroring the engine's ColorType (an AbstractIntType that
    // opts out of ordering only)
    public static final TypeConstructor CONSTRUCTOR = new PrimitiveTypeConstructor("color", ColorType::new, Trait.PRESENT, Trait.ABSENT);
}
