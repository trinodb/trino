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
package io.trino.parquet;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

// same as io.trino.orc.DiskRange
public record DiskRange(long offset, long length)
{
    public DiskRange
    {
        checkArgument(offset >= 0, "offset is negative");
        checkArgument(length > 0, "length must be at least 1");
    }

    public long end()
    {
        return offset + length;
    }

    public boolean contains(DiskRange diskRange)
    {
        return offset <= diskRange.offset() && diskRange.end() <= end();
    }

    /**
     * Returns the minimal DiskRange that encloses both this DiskRange
     * and otherDiskRange. If there was a gap between the ranges the
     * new range will cover that gap.
     */
    public DiskRange span(DiskRange otherDiskRange)
    {
        requireNonNull(otherDiskRange, "otherDiskRange is null");
        long start = Math.min(this.offset, otherDiskRange.offset());
        long end = Math.max(end(), otherDiskRange.end());
        return new DiskRange(start, end - start);
    }
}
