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
package io.trino.plugin.varada.dispatcher.warmup.demoter;

import io.trino.plugin.varada.dispatcher.model.RowGroupKey;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;

import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class RowGroupDataFilter
        implements TupleFilter
{
    private final RowGroupKey rowGroupKey;
    private final Set<WarmUpElement> warmUpElementSet;

    public RowGroupDataFilter(RowGroupKey rowGroupKey, Set<WarmUpElement> warmUpElementSet)
    {
        this.rowGroupKey = requireNonNull(rowGroupKey);
        this.warmUpElementSet = requireNonNull(warmUpElementSet);
    }

    @Override
    public boolean shouldHandle(WarmUpElement warmUpElement, RowGroupKey rowGroupKey)
    {
        return this.rowGroupKey.equals(rowGroupKey) && warmUpElementSet.contains(warmUpElement);
    }

    @Override
    public String toString()
    {
        return "RowGroupDataFilter{" + "rowGroupKey=" + rowGroupKey + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RowGroupDataFilter that = (RowGroupDataFilter) o;
        return Objects.equals(rowGroupKey, that.rowGroupKey) && Objects.equals(warmUpElementSet, that.warmUpElementSet);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rowGroupKey, warmUpElementSet);
    }
}
