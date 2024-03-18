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
package io.trino.plugin.varada.util;

import io.trino.spi.predicate.TupleDomain;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

public class SimplifyResult<T>
{
    private final TupleDomain<T> tupleDomain;
    private final Set<T> simplifiedColumns;

    public SimplifyResult(TupleDomain<T> tupleDomain)
    {
        this(tupleDomain, Collections.emptySet());
    }

    public SimplifyResult(TupleDomain<T> tupleDomain, Set<T> simplifiedColumns)
    {
        this.tupleDomain = tupleDomain;
        this.simplifiedColumns = simplifiedColumns;
    }

    public TupleDomain<T> getTupleDomain()
    {
        return tupleDomain;
    }

    public Set<T> getSimplifiedColumns()
    {
        return simplifiedColumns;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SimplifyResult<?> that)) {
            return false;
        }
        return tupleDomain.equals(that.tupleDomain) && simplifiedColumns.equals(that.simplifiedColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tupleDomain, simplifiedColumns);
    }
}
