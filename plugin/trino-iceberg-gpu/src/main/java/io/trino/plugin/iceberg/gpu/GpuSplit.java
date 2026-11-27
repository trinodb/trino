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
package io.trino.plugin.iceberg.gpu;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A pushed-down aggregation: the whole table's data files, answered by the device at once.
 * {@code key} names the snapshot the files belong to.
 */
public record GpuSplit(String key, List<String> files, long rows)
        implements ConnectorSplit
{
    public GpuSplit
    {
        requireNonNull(key, "key is null");
        files = ImmutableList.copyOf(files);
    }
}
