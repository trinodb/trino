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
package io.trino.lance.file.v2.metadata;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record PageMetadata(long numRows, long priority, PageLayout layout, List<DiskRange> bufferOffsets)
{
    public PageMetadata(long numRows, long priority, PageLayout layout, List<DiskRange> bufferOffsets)
    {
        this.numRows = numRows;
        this.priority = priority;
        this.layout = requireNonNull(layout, "layout is null");
        this.bufferOffsets = requireNonNull(bufferOffsets, "bufferOffsets is null");
    }
}
