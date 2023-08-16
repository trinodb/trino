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

import com.google.inject.Inject;
import io.trino.plugin.hive.metastore.CoralSemiTransactionalHiveMSCAdapter;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.MetadataProvider;
import io.trino.spi.type.TypeManager;

import static io.trino.plugin.hive.HiveSessionProperties.isHiveViewsLegacyTranslation;
import static io.trino.plugin.hive.ViewReaderUtil.HiveViewReader;
import static io.trino.plugin.hive.ViewReaderUtil.PrestoViewReader;
import static io.trino.plugin.hive.ViewReaderUtil.coralTableRedirectionResolver;
import static io.trino.plugin.hive.ViewReaderUtil.isTrinoView;
import static java.util.Objects.requireNonNull;

public class DefaultHiveViewReaderFactory
        implements HiveViewReaderFactory
{
    private final TypeManager typeManager;
    private final MetadataProvider metadataProvider;
    private final boolean runHiveViewRunAsInvoker;
    private final HiveTimestampPrecision hiveTimestampPrecision;

    @Inject
    public DefaultHiveViewReaderFactory(
            TypeManager typeManager,
            MetadataProvider metadataProvider,
            HiveConfig hiveConfig)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.metadataProvider = requireNonNull(metadataProvider, "metadataProvider is null");
        this.runHiveViewRunAsInvoker = hiveConfig.isHiveViewsRunAsInvoker();
        this.hiveTimestampPrecision = hiveConfig.getTimestampPrecision();
    }

    @Override
    public ViewReader createViewReader(SemiTransactionalHiveMetastore metastore, ConnectorSession session, Table table, TableRedirectionResolver tableRedirectionResolver)
    {
        if (isTrinoView(table)) {
            return new PrestoViewReader();
        }
        if (isHiveViewsLegacyTranslation(session)) {
            return new LegacyHiveViewReader(runHiveViewRunAsInvoker);
        }

        return new HiveViewReader(
                new CoralSemiTransactionalHiveMSCAdapter(metastore, coralTableRedirectionResolver(session, tableRedirectionResolver, metadataProvider)),
                typeManager,
                runHiveViewRunAsInvoker,
                hiveTimestampPrecision);
    }
}
