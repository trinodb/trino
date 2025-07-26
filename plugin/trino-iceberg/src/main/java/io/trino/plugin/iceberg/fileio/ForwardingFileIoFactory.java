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
package io.trino.plugin.iceberg.fileio;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.plugin.iceberg.ForIcebergFileDelete;
import org.apache.iceberg.io.FileIO;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

public class ForwardingFileIoFactory
{
    private final ExecutorService deleteExecutor;

    @Inject
    public ForwardingFileIoFactory(@ForIcebergFileDelete ExecutorService deleteExecutor)
    {
        this.deleteExecutor = requireNonNull(deleteExecutor, "deleteExecutor is null");
    }

    public FileIO create(TrinoFileSystem fileSystem)
    {
        return create(fileSystem, true, ImmutableMap.of());
    }

    public FileIO create(TrinoFileSystem fileSystem, boolean useFileSizeFromMetadata)
    {
        return create(fileSystem, useFileSizeFromMetadata, ImmutableMap.of());
    }

    public FileIO create(TrinoFileSystem fileSystem, Map<String, String> properties)
    {
        return create(fileSystem, true, properties);
    }

    public FileIO create(TrinoFileSystem fileSystem, boolean useFileSizeFromMetadata, Map<String, String> properties)
    {
        return new ForwardingFileIo(fileSystem, properties, useFileSizeFromMetadata, deleteExecutor);
    }
}
