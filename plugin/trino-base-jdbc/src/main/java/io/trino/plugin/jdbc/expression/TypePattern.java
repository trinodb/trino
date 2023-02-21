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
package io.trino.plugin.jdbc.expression;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import java.util.Optional;
import java.util.function.Predicate;

public interface TypePattern
{
    static Predicate<Type> getTypePredicate(Optional<TypePattern> optTypePattern)
    {
        return type -> optTypePattern
                .map(typePattern -> typePattern.getPattern()
                        .match(type)
                        .findFirst()
                        .isPresent())
                .orElse(true);
    }

    static Predicate<Type> getArrayTypePredicate(Optional<TypePattern> optElementTypePattern)
    {
        return type -> {
            if (type instanceof ArrayType arrayType) {
                Type elementType = arrayType.getElementType();
                return getTypePredicate(optElementTypePattern).test(elementType);
            }
            else {
                return false;
            }
        };
    }

    Pattern<Type> getPattern();

    void resolve(Captures captures, MatchContext matchContext);
}
