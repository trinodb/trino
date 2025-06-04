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

import com.google.errorprone.annotations.ThreadSafe;
import io.trino.spi.connector.SourcePage;

import static java.util.Objects.requireNonNull;

@ThreadSafe
public interface RowPredicate
{
    boolean test(SourcePage page, int position);

    default RowPredicate and(RowPredicate other)
    {
        requireNonNull(other, "other is null");
        return (page, position) -> test(page, position) && other.test(page, position);
    }

    default void applyFilter(SourcePage page)
    {
        int positionCount = page.getPositionCount();
        int[] retained = new int[positionCount];
        int retainedCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (test(page, position)) {
                retained[retainedCount] = position;
                retainedCount++;
            }
        }
        if (retainedCount != positionCount) {
            page.selectPositions(retained, 0, retainedCount);
        }
    }
}
