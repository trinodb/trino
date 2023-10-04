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
package io.trino.connector.system.jdbc;

import io.airlift.slice.Slice;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Domain.DiscreteSet;

import java.util.Optional;

import static java.util.Locale.ENGLISH;

public final class FilterUtil
{
    private FilterUtil() {}

    public static <T> Optional<String> tryGetSingleVarcharValue(Domain domain)
    {
        if (!domain.isSingleValue()) {
            return Optional.empty();
        }
        Object value = domain.getSingleValue();
        return Optional.of(((Slice) value).toStringUtf8());
    }

    public static QualifiedTablePrefix tablePrefix(String catalog, Optional<String> schema, Optional<String> table)
    {
        if (schema.isPresent() && table.isPresent()) {
            return new QualifiedTablePrefix(catalog, schema.get(), table.get());
        }
        if (schema.isPresent()) {
            return new QualifiedTablePrefix(catalog, schema.get());
        }
        return new QualifiedTablePrefix(catalog);
    }

    public static boolean isImpossibleObjectName(Domain domain)
    {
        if (!domain.isNullableDiscreteSet()) {
            return false;
        }
        DiscreteSet discreteSet = domain.getNullableDiscreteSet();
        return discreteSet.getNonNullValues().stream()
                .allMatch(element -> isImpossibleObjectName(((Slice) element).toStringUtf8()));
    }

    public static boolean isImpossibleObjectName(String candidate)
    {
        return candidate.equals("") ||
                // TODO (https://github.com/trinodb/trino/issues/17) Currently all object names are lowercase in Trino
                !candidate.equals(candidate.toLowerCase(ENGLISH));
    }
}
