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
package io.trino.plugin.kingbasemysql;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.kingbase8.Driver;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcMetadataConfig;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.TimestampTimeZoneDomain;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.sql.SQLException;
import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.jdbc.JdbcModule.bindTablePropertiesProvider;

public class KingbaseMySqlClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(KingbaseMySqlClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfigDefaults(JdbcMetadataConfig.class, config -> config.setBulkListColumns(true));
        newOptionalBinder(binder, TimestampTimeZoneDomain.class).setBinding().toInstance(TimestampTimeZoneDomain.UTC_ONLY);
        configBinder(binder).bindConfig(KingbaseMySqlJdbcConfig.class);
        configBinder(binder).bindConfig(KingbaseMySqlConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        bindTablePropertiesProvider(binder, KingbaseMySqlTableProperties.class);
        install(new DecimalModule());
        install(new JdbcJoinPushdownSupportModule());
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, KingbaseMySqlConfig mySqlConfig, OpenTelemetry openTelemetry)
            throws SQLException
    {
        return DriverConnectionFactory.builder(new Driver(), config.getConnectionUrl(), credentialProvider)
                .setConnectionProperties(getConnectionProperties(mySqlConfig))
                .setOpenTelemetry(openTelemetry)
                .build();
    }

    /**
     * 设置 KingbaseES JDBC 连接属性。仅使用 KES 驱动支持的参数，原 MySQL 专用参数已替换或移除。
     * 参数依据 KingbaseES V9 JDBC 文档：3.1.2 JDBC连接属性（协议/预编译缓存/会话属性/性能扩展/网络控制等表）。
     * 文档地址：https://help.kingbase.com.cn/v9/development/client-interfaces/jdbc/jdbc-2.html
     *
     * 以下 MySQL 参数 KES 无直接对应
     * - useInformationSchema（元数据获取方式）
     * - useUnicode / characterEncoding（已用 clientEncoding 替代）
     * - tinyInt1isBit（MySQL TINYINT(1) 语义）
     * - connectionTimeZone / forceConnectionTimeZoneToSession（会话时区）
     * - autoReconnect / maxReconnects（连接断开自动重连）
     */
    public static Properties getConnectionProperties(KingbaseMySqlConfig mySqlConfig)
    {
        Properties connectionProperties = new Properties();

        // 会话属性：客户端编码（对应 MySQL 的 useUnicode+characterEncoding）
        connectionProperties.setProperty("clientEncoding", "UTF8");

        // 性能扩展：批量 INSERT 重写优化（对应 MySQL 的 rewriteBatchedStatements）
        connectionProperties.setProperty("reWriteBatchedInserts", "true");

        if (mySqlConfig.getConnectionTimeout() != null) {
            // 网络控制：连接超时，单位秒（KES 为秒，MySQL 驱动为毫秒）
            long timeoutSeconds = mySqlConfig.getConnectionTimeout().toMillis() / 1000;
            connectionProperties.setProperty("connectTimeout", String.valueOf(timeoutSeconds <= 0 ? 10 : timeoutSeconds));
        }

        return connectionProperties;
    }
}
