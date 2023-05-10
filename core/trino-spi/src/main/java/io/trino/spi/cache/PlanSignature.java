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
package io.trino.spi.cache;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

/**
 * Plan signature is a normalized and canonicalized representation of subplan.
 * Plan signatures allow to identify, match and adapt similar subqueries.
 * Concept of plan signatures is described in http://www.cs.columbia.edu/~jrzhou/pub/cse.pdf
 */
public class PlanSignature
{
    private static final int INSTANCE_SIZE = instanceSize(PlanSignature.class);

    /**
     * Key of a plan signature. Plans that can be potentially adapted
     * to produce the same results (e.g. using column pruning, filtering or aggregation)
     * will share the same key.
     */
    private final SignatureKey key;
    /**
     * List of group by columns if plan signature represents aggregation.
     */
    private final Optional<List<CacheColumnId>> groupByColumns;
    /**
     * List of output columns.
     */
    private final List<CacheColumnId> columns;
    /**
     * List of output columns types parallel to {@link PlanSignature#columns}.
     */
    private final List<Type> columnsTypes;
    /**
     * Predicate that is enforced on result rows represented by {@link PlanSignature}.
     * You can use output of `PlanSignature A` to derive output of matching `PlanSignature B`
     * as long as `B.predicate` is a strict subset of `A.predicate`. To do so, `B.predicate`
     * must be applied on output of `PlanSignature A`.
     */
    private final TupleDomain<CacheColumnId> predicate;
    /**
     * Predicate derived from dynamic filter that was applied on top of enforced
     * {@link PlanSignature#predicate}. Dynamic predicate is not enforced, but best effort.
     * You can use output of `PlanSignature A` to derive output of matching `PlanSignature B`
     * as long as `B.dynamicPredicate` is a subset of `A.dynamicPredicate`.
     */
    private final TupleDomain<CacheColumnId> dynamicPredicate;

    private volatile int hashCode;

    @JsonCreator
    public PlanSignature(
            SignatureKey key,
            Optional<List<CacheColumnId>> groupByColumns,
            List<CacheColumnId> columns,
            List<Type> columnsTypes,
            TupleDomain<CacheColumnId> predicate,
            TupleDomain<CacheColumnId> dynamicPredicate)
    {
        this.key = requireNonNull(key, "key is null");
        this.groupByColumns = requireNonNull(groupByColumns, "groupByColumns is null").map(List::copyOf);
        this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
        this.columnsTypes = requireNonNull(columnsTypes, "columns types is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.dynamicPredicate = requireNonNull(dynamicPredicate, "dynamicPredicate is null");
    }

    @JsonProperty
    public SignatureKey getKey()
    {
        return key;
    }

    @JsonProperty
    public Optional<List<CacheColumnId>> getGroupByColumns()
    {
        return groupByColumns;
    }

    @JsonProperty
    public List<CacheColumnId> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public TupleDomain<CacheColumnId> getPredicate()
    {
        return predicate;
    }

    @JsonProperty
    public TupleDomain<CacheColumnId> getDynamicPredicate()
    {
        return dynamicPredicate;
    }

    public PlanSignature withDynamicPredicate(TupleDomain<CacheColumnId> dynamicPredicate)
    {
        if (!this.dynamicPredicate.isAll()) {
            throw new IllegalStateException("Dynamic predicate is already set");
        }
        return new PlanSignature(
                key,
                groupByColumns,
                columns,
                columnsTypes,
                predicate,
                dynamicPredicate);
    }

    @JsonProperty
    public List<Type> getColumnsTypes()
    {
        return columnsTypes;
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
        PlanSignature signature = (PlanSignature) o;
        return key.equals(signature.key)
                && groupByColumns.equals(signature.groupByColumns)
                && columns.equals(signature.columns)
                && columnsTypes.equals(signature.columnsTypes)
                && predicate.equals(signature.predicate)
                && dynamicPredicate.equals(signature.dynamicPredicate);
    }

    @Override
    public int hashCode()
    {
        if (hashCode == 0) {
            hashCode = Objects.hash(key, groupByColumns, columns, predicate, dynamicPredicate, columnsTypes);
        }
        return hashCode;
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", PlanSignature.class.getSimpleName() + "[", "]")
                .add("key=" + key)
                .add("groupByColumns=" + groupByColumns)
                .add("columns=" + columns)
                .add("columnTypes=" + columnsTypes)
                .add("predicate=" + predicate)
                .add("dynamicPredicate=" + dynamicPredicate)
                .toString();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + key.getRetainedSizeInBytes()
                + sizeOf(groupByColumns, cols -> estimatedSizeOf(cols, CacheColumnId::getRetainedSizeInBytes))
                + estimatedSizeOf(columns, CacheColumnId::getRetainedSizeInBytes)
                + predicate.getRetainedSizeInBytes(CacheColumnId::getRetainedSizeInBytes)
                + dynamicPredicate.getRetainedSizeInBytes(CacheColumnId::getRetainedSizeInBytes);
    }
}
