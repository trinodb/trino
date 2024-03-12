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
package org.apache.iceberg.snowflake;

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.iceberg.fileio.ForwardingFileIo;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.iceberg.io.FileIO;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TrinoIcebergSnowflakeCatalogFileIOFactory
        extends SnowflakeCatalog.FileIOFactory
{
    private final TrinoFileSystemFactory trinoFileSystemFactory;
    private final ConnectorIdentity identity;

    public TrinoIcebergSnowflakeCatalogFileIOFactory(TrinoFileSystemFactory trinoFileSystemFactory, ConnectorIdentity identity)
    {
        this.trinoFileSystemFactory = requireNonNull(trinoFileSystemFactory, "trinoFileSystemFactory is null");
        this.identity = requireNonNull(identity, "identity is null");
    }

    @Override
    public FileIO newFileIO(String impl, Map<String, String> properties, Object hadoopConf)
    {
        return new ForwardingFileIo(trinoFileSystemFactory.create(identity));
    }
}
