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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergUtil.getPartitionFieldValues;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionRow;
import static java.util.Objects.requireNonNull;

/**
 * Constraints on the {@code $partition} column and the partition fields projected out of it, evaluated
 * against a file's partition.
 */
public final class PartitionFieldPredicate
{
    private static final PartitionFieldPredicate ALL = new PartitionFieldPredicate(ImmutableMap.of(), Optional.empty());

    // partition field id -> domain
    private final Map<Integer, Domain> partitionFieldDomains;
    private final Optional<PartitionRowPredicate> partitionRowPredicate;

    private PartitionFieldPredicate(Map<Integer, Domain> partitionFieldDomains, Optional<PartitionRowPredicate> partitionRowPredicate)
    {
        this.partitionFieldDomains = ImmutableMap.copyOf(requireNonNull(partitionFieldDomains, "partitionFieldDomains is null"));
        this.partitionRowPredicate = requireNonNull(partitionRowPredicate, "partitionRowPredicate is null");
    }

    public static PartitionFieldPredicate fromPredicate(TupleDomain<IcebergColumnHandle> predicate)
    {
        if (predicate.isAll()) {
            return ALL;
        }
        ImmutableMap.Builder<Integer, Domain> partitionFieldDomains = ImmutableMap.builder();
        Optional<PartitionRowPredicate> partitionRowPredicate = Optional.empty();
        for (Map.Entry<IcebergColumnHandle, Domain> entry : predicate.getDomains().orElseThrow(() -> new IllegalArgumentException("Unexpected NONE tuple domain")).entrySet()) {
            IcebergColumnHandle column = entry.getKey();
            if (column.isPartitionField()) {
                partitionFieldDomains.put(column.getId(), entry.getValue());
            }
            else if (column.isPartitionColumn()) {
                partitionRowPredicate = Optional.of(new PartitionRowPredicate(IcebergPartitionColumn.fromColumnHandle(column), entry.getValue()));
            }
        }
        return new PartitionFieldPredicate(partitionFieldDomains.buildOrThrow(), partitionRowPredicate);
    }

    /**
     * Row domains the engine builds hold only rows without null or NaN fields, so membership in the
     * domain agrees with SQL row equality.
     */
    public static boolean canEnforcePartitionRowDomain(Domain domain)
    {
        ValueSet values = domain.getValues();
        // IS NOT NULL prunes nothing, so it does not count as a partition filter
        return values.isNone() || (!values.isAll() && values.isDiscreteSet());
    }

    public boolean isAll()
    {
        return partitionFieldDomains.isEmpty() && partitionRowPredicate.isEmpty();
    }

    public boolean matches(PartitionSpec spec, StructLike partition)
    {
        Map<Integer, Object> partitionFieldValues = partitionRowPredicate.isPresent()
                ? getPartitionFieldValues(spec, partition)
                : getPartitionFieldValues(spec, partition, partitionFieldDomains::containsKey);
        for (Map.Entry<Integer, Domain> entry : partitionFieldDomains.entrySet()) {
            if (!entry.getValue().includesNullableValue(partitionFieldValues.get(entry.getKey()))) {
                return false;
            }
        }
        return partitionRowPredicate.isEmpty() || partitionRowPredicate.get().matches(partitionFieldValues);
    }

    private record PartitionRowPredicate(IcebergPartitionColumn partitionColumn, Domain domain)
    {
        private PartitionRowPredicate
        {
            requireNonNull(partitionColumn, "partitionColumn is null");
            requireNonNull(domain, "domain is null");
        }

        private boolean matches(Map<Integer, Object> partitionFieldValues)
        {
            return domain.includesNullableValue(getPartitionRow(partitionColumn, partitionFieldValues));
        }
    }
}
