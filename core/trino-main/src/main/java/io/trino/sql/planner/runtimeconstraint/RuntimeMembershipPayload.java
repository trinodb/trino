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
package io.trino.sql.planner.runtimeconstraint;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.trino.spi.predicate.Domain;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public record RuntimeMembershipPayload(
        List<Lane> lanes,
        RuntimeConstraintNullMatchMode nullMatchMode,
        boolean sawInputRow)
        implements RuntimeConstraintPayload
{
    public RuntimeMembershipPayload(List<Domain> scalarDomains, RuntimeConstraintNullMatchMode nullMatchMode)
    {
        this(scalarDomains.stream().map(domain -> new Lane(domain, domain.isNullAllowed())).toList(),
                nullMatchMode,
                scalarDomains.stream().anyMatch(domain -> !domain.isNone()));
    }

    public RuntimeMembershipPayload
    {
        lanes = ImmutableList.copyOf(requireNonNull(lanes, "lanes is null"));
        checkArgument(!lanes.isEmpty(), "lanes is empty");
        requireNonNull(nullMatchMode, "nullMatchMode is null");
        checkArgument(sawInputRow || lanes.stream().noneMatch(Lane::sawNull), "empty input cannot contain null");
    }

    public List<Domain> scalarDomains()
    {
        return lanes.stream().map(Lane::domain).toList();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return lanes.stream()
                .map(Lane::domain)
                .mapToLong(Domain::getRetainedSizeInBytes)
                .sum();
    }

    @Immutable
    public record Lane(Domain domain, boolean sawNull)
    {
        public Lane
        {
            requireNonNull(domain, "domain is null");
        }
    }
}
