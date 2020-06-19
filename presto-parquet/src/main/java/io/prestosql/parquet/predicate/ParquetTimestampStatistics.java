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
package io.prestosql.parquet.predicate;

public class ParquetTimestampStatistics
        implements ParquetRangeStatistics<Long>
{
    private final long minimum;
    private final long maximum;

    public ParquetTimestampStatistics(long min, long max)
    {
        this.minimum = min;
        this.maximum = max;
    }

    @Override
    public Long getMin()
    {
        return minimum;
    }

    @Override
    public Long getMax()
    {
        return maximum;
    }
}
