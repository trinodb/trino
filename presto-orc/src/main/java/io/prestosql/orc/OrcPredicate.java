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
package io.prestosql.orc;

import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.statistics.ColumnStatistics;

public interface OrcPredicate
{
    OrcPredicate TRUE = (numberOfRows, statisticsByColumnIndex) -> true;

    /**
     * Should the ORC reader process a file section with the specified statistics.
     *
     * @param numberOfRows the number of rows in the segment; this can be used with
     * {@code ColumnStatistics} to determine if a column is only null
     * @param allColumnStatistics column statistics
     */
    boolean matches(long numberOfRows, ColumnMetadata<ColumnStatistics> allColumnStatistics);
}
