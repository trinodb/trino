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
package io.trino.plugin.varada.dispatcher.query.classifier;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.TreeMultimap;
import io.trino.plugin.varada.dispatcher.model.RegularColumn;
import io.trino.plugin.varada.dispatcher.model.TransformedColumn;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.model.WarmUpElement;
import io.trino.plugin.varada.expression.TransformFunction;
import io.trino.plugin.warp.gen.constants.WarmUpType;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;

public final class WarmedWarmupTypes
{
    private final ImmutableMap<VaradaColumn, WarmUpElement> dataWarmedElements;
    private final ImmutableMap<VaradaColumn, WarmUpElement> luceneWarmedElements;
    private final TreeMultimap<VaradaColumn, WarmUpElement> bloomWarmedElements;
    private final ImmutableListMultimap<VaradaColumn, WarmUpElement> basicWarmedElements;
    private final ImmutableMap<VaradaColumn, Integer> varadaColumnToTotalRecords;
    private final ImmutableSet<VaradaColumn> warmedColumns;

    private WarmedWarmupTypes(
            ImmutableMap<VaradaColumn, WarmUpElement> dataWarmedElements,
            ImmutableMap<VaradaColumn, WarmUpElement> luceneWarmedElements,
            TreeMultimap<VaradaColumn, WarmUpElement> bloomWarmedElements,
            ImmutableListMultimap<VaradaColumn, WarmUpElement> basicWarmedElements,
            ImmutableMap<VaradaColumn, Integer> varadaColumnToTotalRecords,
            ImmutableSet<VaradaColumn> warmedColumns)
    {
        this.dataWarmedElements = dataWarmedElements;
        this.luceneWarmedElements = luceneWarmedElements;
        this.bloomWarmedElements = bloomWarmedElements;
        this.basicWarmedElements = basicWarmedElements;
        this.varadaColumnToTotalRecords = varadaColumnToTotalRecords;
        this.warmedColumns = warmedColumns;
    }

    public ImmutableMap<VaradaColumn, WarmUpElement> dataWarmedElements()
    {
        return dataWarmedElements;
    }

    public ImmutableMap<VaradaColumn, WarmUpElement> luceneWarmedElements()
    {
        return luceneWarmedElements;
    }

    public TreeMultimap<VaradaColumn, WarmUpElement> bloomWarmedElements()
    {
        return bloomWarmedElements;
    }

    public ImmutableListMultimap<VaradaColumn, WarmUpElement> basicWarmedElements()
    {
        return basicWarmedElements;
    }

    public Optional<WarmUpElement> getByTypeAndColumn(WarmUpType warmUpType, VaradaColumn varadaColumn, TransformFunction transformFunction)
    {
        return switch (warmUpType) {
            case WARM_UP_TYPE_DATA -> Optional.ofNullable(dataWarmedElements.get(varadaColumn));
            case WARM_UP_TYPE_LUCENE -> Optional.ofNullable(luceneWarmedElements.get(varadaColumn));
            case WARM_UP_TYPE_BASIC -> basicWarmedElements.get(varadaColumn).stream()
                    .filter(x ->
                            ((x.getVaradaColumn() instanceof TransformedColumn transformedColumn) &&
                                    Objects.equals(transformedColumn.getTransformFunction(), transformFunction)) ||
                            (!x.getVaradaColumn().isTransformedColumn() &&
                                    Objects.equals(transformFunction, TransformFunction.NONE)))
                    .findFirst();
            case WARM_UP_TYPE_BLOOM_HIGH, WARM_UP_TYPE_BLOOM_MEDIUM, WARM_UP_TYPE_BLOOM_LOW ->
                    bloomWarmedElements.get(varadaColumn).isEmpty() ? Optional.empty() : Optional.ofNullable(bloomWarmedElements.get(varadaColumn).pollFirst());
            case WARM_UP_TYPE_NUM_OF -> throw new RuntimeException();
        };
    }

    public boolean contains(VaradaColumn varadaColumn, WarmUpType warmUpType, TransformFunction transformFunction)
    {
        return switch (warmUpType) {
            case WARM_UP_TYPE_DATA -> dataWarmedElements.get(varadaColumn) != null;
            case WARM_UP_TYPE_LUCENE -> luceneWarmedElements.get(varadaColumn) != null;
            case WARM_UP_TYPE_BASIC -> basicWarmedElements.containsKey(varadaColumn) &&
                    (((basicWarmedElements.get(varadaColumn).stream().findFirst().get().getVaradaColumn() instanceof TransformedColumn transformedColumn) &&
                            Objects.equals(transformedColumn.getTransformFunction(), transformFunction)) ||
                            (!basicWarmedElements.get(varadaColumn).stream().findFirst().get().getVaradaColumn().isTransformedColumn() &&
                                    Objects.equals(transformFunction, TransformFunction.NONE)));
            case WARM_UP_TYPE_BLOOM_HIGH, WARM_UP_TYPE_BLOOM_MEDIUM, WARM_UP_TYPE_BLOOM_LOW -> bloomWarmedElements.containsKey(varadaColumn);
            case WARM_UP_TYPE_NUM_OF -> throw new RuntimeException();
        };
    }

    public ImmutableSet<VaradaColumn> getWarmedColumns()
    {
        return warmedColumns;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (WarmedWarmupTypes) obj;
        return Objects.equals(this.dataWarmedElements, that.dataWarmedElements) &&
                Objects.equals(this.luceneWarmedElements, that.luceneWarmedElements) &&
                Objects.equals(this.bloomWarmedElements, that.bloomWarmedElements) &&
                Objects.equals(this.basicWarmedElements, that.basicWarmedElements);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(dataWarmedElements, luceneWarmedElements, bloomWarmedElements, basicWarmedElements);
    }

    @Override
    public String toString()
    {
        return "WarmedWarmupTypes[" +
                "warmedColumns=" + warmedColumns + ", " +
                "varadaColumnToTotalRecords=" + varadaColumnToTotalRecords + ", " +
                "dataWarmedElements=" + dataWarmedElements + ", " +
                "luceneWarmedElements=" + luceneWarmedElements + ", " +
                "bloomWarmedElements=" + bloomWarmedElements + ", " +
                "basicWarmedElements=" + basicWarmedElements + ']';
    }

    public boolean isNewColumn(VaradaColumn varadaColumn)
    {
        return !getWarmedColumns().contains(varadaColumn);
    }

    /**
     * get total records from a valid warmup element
     */
    public OptionalInt getColumnTotalRecords(RegularColumn regularColumn)
    {
        Integer totalRecords = varadaColumnToTotalRecords.get(regularColumn);
        return totalRecords == null ? OptionalInt.empty() : OptionalInt.of(totalRecords);
    }

    public static class Builder
    {
        private final ImmutableMap.Builder<VaradaColumn, WarmUpElement> dataWarmedElements = ImmutableMap.builder();
        private final ImmutableMap.Builder<VaradaColumn, WarmUpElement> luceneWarmedElements = ImmutableMap.builder();
        private final TreeMultimap<VaradaColumn, WarmUpElement> bloomWarmedElements = TreeMultimap.create(Comparator.comparing(VaradaColumn::toString), Comparator.comparing(WarmUpElement::getWarmUpType));
        private final ImmutableListMultimap.Builder<VaradaColumn, WarmUpElement> basicWarmedElements = ImmutableListMultimap.builder();

        private final Map<VaradaColumn, Integer> varadaColumnToTotalRecords = new HashMap<>();

        private final ImmutableSet.Builder<VaradaColumn> warmedColumns = ImmutableSet.builder();

        public void add(WarmUpElement we)
        {
            switch (we.getWarmUpType()) {
                case WARM_UP_TYPE_DATA -> dataWarmedElements.put(we.getVaradaColumn(), we);
                case WARM_UP_TYPE_LUCENE -> luceneWarmedElements.put(we.getVaradaColumn(), we);
                case WARM_UP_TYPE_BASIC -> basicWarmedElements.put(we.getVaradaColumn(), we);
                case WARM_UP_TYPE_BLOOM_HIGH, WARM_UP_TYPE_BLOOM_MEDIUM, WARM_UP_TYPE_BLOOM_LOW -> bloomWarmedElements.put(we.getVaradaColumn(), we);
                case WARM_UP_TYPE_NUM_OF -> throw new RuntimeException();
            }
            warmedColumns.add(we.getVaradaColumn());
            if (we.isValid()) {
                Integer previousValue = varadaColumnToTotalRecords.putIfAbsent(we.getVaradaColumn(), we.getTotalRecords());
                checkArgument(previousValue == null || previousValue == we.getTotalRecords(), "previousValue=%s, weTotalRecords=%s, weWarmUpType=%s, weVaradaColumn=%s", previousValue, we.getTotalRecords(), we.getWarmUpType(), we.getVaradaColumn());
            }
        }

        public WarmedWarmupTypes build()
        {
            return new WarmedWarmupTypes(
                    dataWarmedElements.buildOrThrow(),
                    luceneWarmedElements.buildOrThrow(),
                    bloomWarmedElements,
                    basicWarmedElements.build(),
                    ImmutableMap.copyOf(varadaColumnToTotalRecords),
                    warmedColumns.build());
        }
    }
}
