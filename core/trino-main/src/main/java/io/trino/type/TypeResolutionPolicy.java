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
package io.trino.type;

import static java.util.Objects.requireNonNull;

/// Session-dependent type resolution choices. Include the complete policy in resolution cache
/// keys so sessions using different inference engines or coercion rules remain independent.
public record TypeResolutionPolicy(CharVarcharCoercion charVarcharCoercion, boolean legacyTypeResolver)
{
    public static final TypeResolutionPolicy SQL_STANDARD = new TypeResolutionPolicy(CharVarcharCoercion.SQL_STANDARD, false);
    public static final TypeResolutionPolicy LEGACY = new TypeResolutionPolicy(CharVarcharCoercion.LEGACY, false);

    public TypeResolutionPolicy
    {
        requireNonNull(charVarcharCoercion, "charVarcharCoercion is null");
    }

    public boolean legacyCharCoercion()
    {
        return charVarcharCoercion == CharVarcharCoercion.LEGACY;
    }
}
