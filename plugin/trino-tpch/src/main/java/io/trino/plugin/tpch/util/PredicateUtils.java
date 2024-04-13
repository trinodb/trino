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
package io.trino.plugin.tpch.util;

import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;

import java.util.function.Predicate;

public final class PredicateUtils
{
    private PredicateUtils() {}

    public static Predicate<NullableValue> convertToPredicate(TupleDomain<ColumnHandle> predicate, TpchColumnHandle columnHandle)
    {
        Domain columnDomain = predicate.getDomains()
                .map(domains -> domains.getOrDefault(columnHandle, Domain.all(columnHandle.type())))
                .orElseGet(() -> Domain.none(columnHandle.type()));

        return nullableValue -> columnDomain.includesNullableValue(nullableValue.getValue());
    }

    public static TupleDomain<ColumnHandle> filterOutColumnFromPredicate(TupleDomain<ColumnHandle> predicate, TpchColumnHandle columnHandle)
    {
        return filterColumns(predicate, tpchColumnHandle -> !tpchColumnHandle.equals(columnHandle));
    }

    private static TupleDomain<ColumnHandle> filterColumns(TupleDomain<ColumnHandle> predicate, Predicate<TpchColumnHandle> filterPredicate)
    {
        return predicate.filter((columnHandle, domain) -> filterPredicate.test((TpchColumnHandle) columnHandle));
    }
}
