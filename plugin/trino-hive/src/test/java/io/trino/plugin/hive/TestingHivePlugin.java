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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.trino.metastore.HiveMetastore;
import io.trino.parquet.crypto.DecryptionKeyRetriever;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.nio.file.Path;
import java.util.Optional;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static java.util.Objects.requireNonNull;

public class TestingHivePlugin
        implements Plugin
{
    private final Path localFileSystemRootPath;
    private final Optional<HiveMetastore> metastore;
    private final boolean metastoreImpersonationEnabled;
    private final Optional<DecryptionKeyRetriever> decryptionKeyRetriever;
    private final Module additionalModule;

    public TestingHivePlugin(Path localFileSystemRootPath)
    {
        this(localFileSystemRootPath, Optional.empty(), false, Optional.empty(), EMPTY_MODULE);
    }

    public TestingHivePlugin(Path localFileSystemRootPath, Module additionalModule)
    {
        this(localFileSystemRootPath, Optional.empty(), false, Optional.empty(), additionalModule);
    }

    @Deprecated
    public TestingHivePlugin(Path localFileSystemRootPath, HiveMetastore metastore)
    {
        this(localFileSystemRootPath, Optional.of(metastore), false, Optional.empty(), EMPTY_MODULE);
    }

    @Deprecated
    public TestingHivePlugin(
            Path localFileSystemRootPath,
            Optional<HiveMetastore> metastore,
            boolean metastoreImpersonationEnabled,
            Optional<DecryptionKeyRetriever> decryptionKeyRetriever)
    {
        this(localFileSystemRootPath, metastore, metastoreImpersonationEnabled, decryptionKeyRetriever, EMPTY_MODULE);
    }

    public TestingHivePlugin(
            Path localFileSystemRootPath,
            Optional<HiveMetastore> metastore,
            boolean metastoreImpersonationEnabled,
            Optional<DecryptionKeyRetriever> decryptionKeyRetriever,
            Module additionalModule)
    {
        this.localFileSystemRootPath = requireNonNull(localFileSystemRootPath, "localFileSystemRootPath is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.metastoreImpersonationEnabled = metastoreImpersonationEnabled;
        this.decryptionKeyRetriever = requireNonNull(decryptionKeyRetriever, "decryptionKeyRetriever is null");
        this.additionalModule = requireNonNull(additionalModule, "additionalModule is null");
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(new TestingHiveConnectorFactory(localFileSystemRootPath, metastore, metastoreImpersonationEnabled, decryptionKeyRetriever, additionalModule));
    }
}
