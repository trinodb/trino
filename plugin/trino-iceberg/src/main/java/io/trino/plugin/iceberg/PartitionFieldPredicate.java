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
import io.trino.spi.block.SqlRow;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TypeOperators;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;

import java.lang.invoke.MethodHandle;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionFieldValues;
import static io.trino.plugin.iceberg.IcebergUtil.getPartitionRow;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static java.lang.Boolean.TRUE;
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

    public static PartitionFieldPredicate fromPredicate(TupleDomain<IcebergColumnHandle> predicate, TypeOperators typeOperators)
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
                partitionRowPredicate = Optional.of(new PartitionRowPredicate(IcebergPartitionColumn.fromColumnHandle(column), entry.getValue(), typeOperators));
            }
        }
        return new PartitionFieldPredicate(partitionFieldDomains.buildOrThrow(), partitionRowPredicate);
    }

    /**
     * Row domains the engine builds hold only rows without null or NaN fields, so SQL row equality
     * selects exactly the files in the domain.
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

    private static final class PartitionRowPredicate
    {
        private final IcebergPartitionColumn partitionColumn;
        private final Domain domain;
        private final MethodHandle equalOperator;

        private PartitionRowPredicate(IcebergPartitionColumn partitionColumn, Domain domain, TypeOperators typeOperators)
        {
            this.partitionColumn = requireNonNull(partitionColumn, "partitionColumn is null");
            this.domain = requireNonNull(domain, "domain is null");
            this.equalOperator = typeOperators.getEqualOperator(partitionColumn.rowType(), simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL));
        }

        // $partition is never null. Row equality is unknown at a null field, as in the engine
        private boolean matches(Map<Integer, Object> partitionFieldValues)
        {
            if (domain.getValues().isNone()) {
                return false;
            }
            SqlRow row = getPartitionRow(partitionColumn, partitionFieldValues);
            for (Object value : domain.getValues().getDiscreteSet()) {
                if (TRUE.equals(invoke(equalOperator, row, (SqlRow) value))) {
                    return true;
                }
            }
            return false;
        }

        private static Boolean invoke(MethodHandle operator, SqlRow left, SqlRow right)
        {
            try {
                return (Boolean) operator.invoke(left, right);
            }
            catch (Throwable throwable) {
                throwIfUnchecked(throwable);
                throw new RuntimeException(throwable);
            }
        }
    }
}
