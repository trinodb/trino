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
package io.trino.plugin.iceberg;

import com.google.inject.Inject;
import io.trino.spi.NodeVersion;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.TrinoInternalData;

import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.FileFormat.PARQUET;

/**
 * Makes the Iceberg library read and write metadata files, like partition statistics files, in Parquet with
 * Trino's reader and writer. The library's own Parquet entry needs {@code parquet-hadoop}, which is not on the plugin
 * class path. The library has no ORC entry, so ORC internal data is not supported.
 * <p>
 * The library keeps one entry per format for the plugin class loader, so these replace its Parquet entry for all the
 * internal data the plugin reads and writes in Parquet, not only partition statistics files. Format version 4
 * manifests in Parquet would use them too, and need file metadata, maps and lists, which they reject; the connector
 * supports format versions up to 3. Iceberg's public {@code FormatModelRegistry} is where the library makes file
 * formats pluggable, but it covers data files only, and {@code InternalData} does not use it, so the entries are
 * registered through {@link TrinoInternalData}.
 */
public class IcebergInternalData
{
    @Inject
    public IcebergInternalData(TypeManager typeManager, NodeVersion nodeVersion)
    {
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(nodeVersion, "nodeVersion is null");
        // the registry is global, and the entries do not depend on the catalog, so registering again for another catalog changes nothing
        TrinoInternalData.register(
                PARQUET,
                outputFile -> new IcebergInternalDataWriteBuilder(outputFile, typeManager, nodeVersion),
                inputFile -> new IcebergInternalDataReadBuilder(inputFile, typeManager));
    }
}
