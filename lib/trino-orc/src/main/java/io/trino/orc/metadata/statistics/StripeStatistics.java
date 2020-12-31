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
package io.prestosql.orc.metadata.statistics;

import io.prestosql.orc.metadata.ColumnMetadata;
import org.openjdk.jol.info.ClassLayout;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class StripeStatistics
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(StripeStatistics.class).instanceSize();

    private final ColumnMetadata<ColumnStatistics> columnStatistics;
    private final long retainedSizeInBytes;

    public StripeStatistics(ColumnMetadata<ColumnStatistics> columnStatistics)
    {
        this.columnStatistics = requireNonNull(columnStatistics, "columnStatistics is null");
        this.retainedSizeInBytes = INSTANCE_SIZE + columnStatistics.stream().mapToLong(ColumnStatistics::getRetainedSizeInBytes).sum();
    }

    public ColumnMetadata<ColumnStatistics> getColumnStatistics()
    {
        return columnStatistics;
    }

    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
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
        StripeStatistics that = (StripeStatistics) o;
        return Objects.equals(columnStatistics, that.columnStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnStatistics);
    }
}
