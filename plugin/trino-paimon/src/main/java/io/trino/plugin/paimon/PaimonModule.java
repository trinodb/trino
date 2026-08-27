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
package io.trino.plugin.paimon;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.paimon.functions.PaimonFunctionProvider;
import io.trino.plugin.paimon.functions.tablechanges.TableChangesFunctionProcessorProvider;
import io.trino.plugin.paimon.functions.tablechanges.TableChangesFunctionProvider;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import org.apache.paimon.options.Options;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class PaimonModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(PaimonMetadataFactory.class).in(SINGLETON);
        binder.bind(PaimonSplitManager.class).in(SINGLETON);
        binder.bind(PaimonPageSourceProvider.class).in(SINGLETON);
        binder.bind(PaimonPageSinkProvider.class).in(SINGLETON);
        binder.bind(PaimonConnectorStats.class).in(SINGLETON);
        binder.bind(PaimonNodePartitioningProvider.class).in(SINGLETON);
        binder.bind(PaimonSessionProperties.class).in(SINGLETON);
        binder.bind(PaimonSchemaProperties.class).in(SINGLETON);
        binder.bind(PaimonTableOptions.class).in(SINGLETON);
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(TableChangesFunctionProvider.class)
                .in(Scopes.SINGLETON);
        binder.bind(FunctionProvider.class).to(PaimonFunctionProvider.class).in(Scopes.SINGLETON);
        binder.bind(TableChangesFunctionProcessorProvider.class).in(SINGLETON);

        // Bind Paimon-specific configuration
        configBinder(binder).bindConfig(PaimonConfig.class);

        // Bind ORC and Parquet reader configurations
        configBinder(binder).bindConfig(OrcReaderConfig.class);
        configBinder(binder).bindConfig(OrcWriterConfig.class);
        configBinder(binder).bindConfig(ParquetReaderConfig.class);
        configBinder(binder).bindConfig(ParquetWriterConfig.class);
    }

    /**
     * Provides Paimon Options object by converting from PaimonConfig.
     * This method is called by Guice to create the Options bean.
     */
    @Provides
    @Singleton
    public Options provideOptions(PaimonConfig config)
    {
        return config.toOptions();
    }
}
