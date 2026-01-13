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
package io.trino.plugin.kingbaseoracle;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.kingbase8.Driver;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.ForJdbcClient;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.MaxDomainCompactionThreshold;
import io.trino.plugin.jdbc.RetryStrategy;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;
import static io.trino.plugin.kingbaseoracle.KingbaseOracleClient.ORACLE_MAX_LIST_EXPRESSIONS;

public class KingbaseOracleClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(KingbaseOracleClient.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, TimestampTimeZoneDomain.class).setBinding().toInstance(TimestampTimeZoneDomain.ANY);
        bindSessionPropertiesProvider(binder, KingbaseOracleSessionProperties.class);
        configBinder(binder).bindConfig(KingbaseOracleConfig.class);
        newOptionalBinder(binder, Key.get(int.class, MaxDomainCompactionThreshold.class)).setBinding().toInstance(ORACLE_MAX_LIST_EXPRESSIONS);
        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setBulkListColumns(true));
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
        newSetBinder(binder, RetryStrategy.class).addBinding().to(KingbaseOracleRetryStrategy.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, Key.get(ExecutorService.class, ForJdbcClient.class))
                .setBinding()
                .toInstance(MoreExecutors.newDirectExecutorService());
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory connectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, KingbaseOracleConfig kingbaseoracleConfig, OpenTelemetry openTelemetry)
            throws SQLException
    {
        Properties connectionProperties = new Properties();
//        connectionProperties.setProperty(OracleConnection.CONNECTION_PROPERTY_INCLUDE_SYNONYMS, String.valueOf(kingbaseoracleConfig.isSynonymsEnabled()));
//        connectionProperties.setProperty(OracleConnection.CONNECTION_PROPERTY_REPORT_REMARKS, String.valueOf(kingbaseoracleConfig.isRemarksReportingEnabled()));
        connectionProperties.setProperty("loginTimeout", "30");
        connectionProperties.setProperty("socketTimeout", "300");
        connectionProperties.setProperty("defaultRowFetchSize", "1000");

        return DriverConnectionFactory.builder(new Driver(), config.getConnectionUrl(), credentialProvider)
                .setConnectionProperties(connectionProperties)
                .setOpenTelemetry(openTelemetry)
                .build();
    }

    private static class KingbaseOracleRetryStrategy
            implements RetryStrategy
    {
        @Override
        public boolean isExceptionRecoverable(Throwable exception)
        {
            return Throwables.getCausalChain(exception).stream()
                    .anyMatch(SQLRecoverableException.class::isInstance);
        }
    }
}
