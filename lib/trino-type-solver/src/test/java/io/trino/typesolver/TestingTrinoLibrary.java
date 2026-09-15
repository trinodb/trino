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
package io.trino.typesolver;

import io.trino.typesolver.TrinoPreset.CharVarcharCoercion;

import static io.trino.typesolver.TrinoPreset.castRules;
import static io.trino.typesolver.TrinoPreset.coercionRules;
import static io.trino.typesolver.TrinoPreset.typeConstructors;
import static io.trino.typesolver.TrinoPreset.typeSystem;

/// Handwritten function and operator fixtures for solver unit tests.
/// Engine parity is checked against real catalog signatures in trino-main.
final class TestingTrinoLibrary
{
    private TestingTrinoLibrary() {}

    public static TypeLibrary.Builder install(TypeLibrary.Builder builder)
    {
        return install(builder, CharVarcharCoercion.SQL_STANDARD);
    }

    public static TypeLibrary.Builder install(TypeLibrary.Builder builder, CharVarcharCoercion charVarcharCoercion)
    {
        typeConstructors().forEach(builder::registerType);
        coercionRules(charVarcharCoercion).forEach(builder::registerCoercion);
        castRules().forEach(builder::registerCast);
        TrinoOperators.register(builder);
        TrinoScalarFunctions.register(builder);
        builder.withSpecificity(Specificity.BY_COERCION_COUNT.then(new TrinoSpecificity(typeSystem(charVarcharCoercion))));
        return builder;
    }

    public static TypeLibrary library()
    {
        return library(CharVarcharCoercion.SQL_STANDARD);
    }

    public static TypeLibrary library(CharVarcharCoercion charVarcharCoercion)
    {
        return install(TypeLibrary.builder(), charVarcharCoercion).build();
    }
}
