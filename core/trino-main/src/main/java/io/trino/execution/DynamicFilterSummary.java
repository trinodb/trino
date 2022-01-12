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
package io.trino.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.predicate.Domain;
import io.trino.sql.planner.plan.DynamicFilterId;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.execution.SummaryInfo.Type.DYNAMIC_FILTER;
import static java.util.Objects.requireNonNull;

public final class DynamicFilterSummary
        implements SummaryInfo
{
    private final Map<DynamicFilterId, Domain> dynamicFilterDomains;

    @JsonCreator
    public DynamicFilterSummary(Map<DynamicFilterId, Domain> dynamicFilterDomains)
    {
        this.dynamicFilterDomains = ImmutableMap.copyOf(requireNonNull(dynamicFilterDomains, "dynamicFilterDomains is null"));
    }

    @JsonProperty
    public Map<DynamicFilterId, Domain> getDynamicFilterDomains()
    {
        return dynamicFilterDomains;
    }

    @Override
    public boolean isEmpty()
    {
        return dynamicFilterDomains.isEmpty();
    }

    @Override
    public Type getType()
    {
        return DYNAMIC_FILTER;
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
        DynamicFilterSummary that = (DynamicFilterSummary) o;
        return Objects.equals(dynamicFilterDomains, that.dynamicFilterDomains);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(dynamicFilterDomains);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("dynamicFilterDomains", dynamicFilterDomains)
                .toString();
    }
}
