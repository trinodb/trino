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
package io.trino.plugin.hudi.query;

import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import org.apache.hudi.common.model.FileSlice;

import java.io.Closeable;
import java.util.List;
import java.util.stream.Stream;

public interface HudiDirectoryLister
        extends Closeable
{
    Stream<FileSlice> listStatus(HudiPartitionInfo partitionInfo, boolean useIndex);

    /**
     * Hint the lister with the set of partitions known to be relevant for this query so that
     * the underlying file system view can be loaded for only those partitions instead of all.
     * Must be called before the first {@link #listStatus} invocation. Default is a no-op.
     */
    default void setPrunedPartitionPaths(List<String> relativePartitionPaths) {}
}
