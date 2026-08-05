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
package io.trino.plugin.iceberg.delete;

import io.trino.spi.connector.SourcePage;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public interface PageFilter
{
    Positions filterPositions(SourcePage page, Positions positions);

    default void applyFilter(SourcePage page)
    {
        int positionCount = page.getPositionCount();
        Positions retained = filterPositions(page, Positions.range(positionCount));
        if (retained.size() != positionCount) {
            page.selectPositions(retained.getPositions(), 0, retained.size());
        }
    }

    static Optional<PageFilter> allOf(List<PageFilter> filters)
    {
        if (filters.isEmpty()) {
            return Optional.empty();
        }
        if (filters.size() == 1) {
            return Optional.of(filters.get(0));
        }
        PageFilter[] filterArray = filters.toArray(new PageFilter[0]);
        return Optional.of((page, positions) -> {
            Positions retained = positions;
            for (PageFilter filter : filterArray) {
                retained = filter.filterPositions(page, retained);
                if (retained.isEmpty()) {
                    break;
                }
            }
            return retained;
        });
    }

    static PageFilter of(PositionFilter positionFilter)
    {
        requireNonNull(positionFilter, "positionFilter is null");
        return (page, positions) -> positions.filter(position -> positionFilter.test(page, position));
    }

    interface PositionFilter
    {
        boolean test(SourcePage page, int position);
    }
}
