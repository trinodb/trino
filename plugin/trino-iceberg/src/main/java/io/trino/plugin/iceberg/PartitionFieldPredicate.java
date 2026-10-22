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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;

import java.util.Map;

import static io.trino.plugin.iceberg.IcebergUtil.getPartitionFieldValues;
import static java.util.Objects.requireNonNull;

/**
 * Constraints on the partition fields projected out of the {@code $partition} column, evaluated against a file's partition.
 * Constraints on the whole row are not enforced, as row comparison semantics differ from domain membership, so the engine evaluates them.
 */
public final class PartitionFieldPredicate
{
    private static final PartitionFieldPredicate ALL = new PartitionFieldPredicate(ImmutableMap.of());

    // partition field id -> domain
    private final Map<Integer, Domain> partitionFieldDomains;

    private PartitionFieldPredicate(Map<Integer, Domain> partitionFieldDomains)
    {
        this.partitionFieldDomains = ImmutableMap.copyOf(requireNonNull(partitionFieldDomains, "partitionFieldDomains is null"));
    }

    public static PartitionFieldPredicate fromPredicate(TupleDomain<IcebergColumnHandle> predicate)
    {
        if (predicate.isAll()) {
            return ALL;
        }
        ImmutableMap.Builder<Integer, Domain> partitionFieldDomains = ImmutableMap.builder();
        for (Map.Entry<IcebergColumnHandle, Domain> entry : predicate.getDomains().orElseThrow(() -> new IllegalArgumentException("Unexpected NONE tuple domain")).entrySet()) {
            IcebergColumnHandle column = entry.getKey();
            if (column.isPartitionField()) {
                partitionFieldDomains.put(column.getId(), entry.getValue());
            }
        }
        return new PartitionFieldPredicate(partitionFieldDomains.buildOrThrow());
    }

    public boolean isAll()
    {
        return partitionFieldDomains.isEmpty();
    }

    public boolean matches(PartitionSpec spec, StructLike partition)
    {
        Map<Integer, Object> partitionFieldValues = getPartitionFieldValues(spec, partition);
        for (Map.Entry<Integer, Domain> entry : partitionFieldDomains.entrySet()) {
            if (!entry.getValue().includesNullableValue(partitionFieldValues.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }
}
