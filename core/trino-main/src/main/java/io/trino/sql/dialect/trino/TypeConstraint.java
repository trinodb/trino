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
package io.trino.sql.dialect.trino;

import io.trino.spi.type.EmptyRowType;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

import static io.trino.sql.newir.FormatOptions.isValidIdentifier;
import static java.util.Objects.requireNonNull;

public record TypeConstraint(Predicate<Type> constraint)
{
    // Intermediate result row type.
    // Row without fields is supported and represented as EmptyRowType.
    // If row fields are present, they must have valid unique names.
    public static final TypeConstraint IS_RELATION_ROW = new TypeConstraint(type -> {
        if (type instanceof EmptyRowType) {
            return true;
        }
        if (type instanceof RowType rowType) {
            Set<String> uniqueFieldNames = new HashSet<>();
            for (RowType.Field field : rowType.getFields()) {
                if (field.getName().isEmpty()) {
                    return false;
                }
                String fieldName = field.getName().orElseThrow();
                if (!isValidIdentifier(fieldName)) {
                    return false;
                }
                if (!uniqueFieldNames.add(fieldName)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    });

    // Intermediate result type.
    public static final TypeConstraint IS_RELATION = new TypeConstraint(
            type -> type instanceof MultisetType multisetType && IS_RELATION_ROW.test(multisetType.getElementType()));

    public TypeConstraint
    {
        requireNonNull(constraint, "constraint is null");
    }

    public boolean test(Type t)
    {
        return constraint.test(t);
    }
}
