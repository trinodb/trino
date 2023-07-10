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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.base.projection.ApplyProjectionUtil.ProjectedColumnRepresentation;

public final class HiveApplyProjectionUtil
{
    private HiveApplyProjectionUtil() {}

    /**
     * Returns the assignment key corresponding to the column represented by {@param projectedColumn} in the {@param assignments}, if one exists.
     * The variable in the {@param projectedColumn} can itself be a representation of another projected column. For example,
     * say a projected column representation has variable "x" and a dereferenceIndices=[0]. "x" can in-turn map to a projected
     * column handle with base="a" and [1, 2] as dereference indices. Then the method searches for a column handle in
     * {@param assignments} with base="a" and dereferenceIndices=[1, 2, 0].
     */
    public static Optional<String> find(Map<String, ColumnHandle> assignments, ProjectedColumnRepresentation projectedColumn)
    {
        HiveColumnHandle variableColumn = (HiveColumnHandle) assignments.get(projectedColumn.getVariable().getName());

        if (variableColumn == null) {
            return Optional.empty();
        }

        String baseColumnName = variableColumn.getBaseColumnName();

        List<Integer> variableColumnIndices = variableColumn.getHiveColumnProjectionInfo()
                .map(HiveColumnProjectionInfo::getDereferenceIndices)
                .orElse(ImmutableList.of());

        List<Integer> projectionIndices = ImmutableList.<Integer>builder()
                .addAll(variableColumnIndices)
                .addAll(projectedColumn.getDereferenceIndices())
                .build();

        for (Map.Entry<String, ColumnHandle> entry : assignments.entrySet()) {
            HiveColumnHandle column = (HiveColumnHandle) entry.getValue();
            if (column.getBaseColumnName().equals(baseColumnName) &&
                    column.getHiveColumnProjectionInfo()
                            .map(HiveColumnProjectionInfo::getDereferenceIndices)
                            .orElse(ImmutableList.of())
                            .equals(projectionIndices)) {
                return Optional.of(entry.getKey());
            }
        }

        return Optional.empty();
    }
}
