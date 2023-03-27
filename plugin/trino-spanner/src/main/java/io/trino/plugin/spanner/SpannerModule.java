package io.trino.plugin.spanner;

import com.google.cloud.spanner.jdbc.JdbcDriver;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConfiguringConnectionFactory;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DecimalModule;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcJoinPushdownSupportModule;
import io.trino.plugin.jdbc.JdbcStatisticsConfig;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.RemoteQueryCancellationModule;
import io.trino.plugin.jdbc.TablePropertiesProvider;
import io.trino.plugin.jdbc.credential.CredentialProvider;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.ptf.Query;
import io.trino.spi.ptf.ConnectorTableFunction;
import io.trino.spi.type.TypeManager;

import java.io.File;
import java.util.Properties;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class SpannerModule
        extends AbstractConfigurationAwareModule
{

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config,
            CredentialProvider credentialProvider,
            SpannerConfig spannerConfig)
            throws ClassNotFoundException
    {
        Class.forName("com.google.cloud.spanner.jdbc.JdbcDriver");
        Properties connectionProperties = new Properties();
        String connectionUrl = config.getConnectionUrl();
        JdbcDriver driver = new JdbcDriver();
        //File credentials = new File(spannerConfig.getCredentialsFile());
        if (!driver.acceptsURL(connectionUrl)) {
            throw new RuntimeException(config.getConnectionUrl() + " is incorrect");
        }
        //connectionProperties.put("credentials", spannerConfig.getCredentialsFile());
        return new ConfiguringConnectionFactory(new DriverConnectionFactory(
                driver,
                config.getConnectionUrl(),
                connectionProperties,
                credentialProvider),
                connection -> {
                });
    }

    @Provides
    @Singleton
    public static SpannerClient getSpannerClient(
            BaseJdbcConfig config,
            SpannerConfig spannerConfig,
            JdbcStatisticsConfig statisticsConfig,
            ConnectionFactory connectionFactory,
            QueryBuilder queryBuilder,
            TypeManager typeManager,
            IdentifierMapping identifierMapping,
            RemoteQueryModifier queryModifier)
    {
        return new SpannerClient(config,
                spannerConfig,
                statisticsConfig,
                connectionFactory,
                queryBuilder,
                typeManager,
                identifierMapping,
                queryModifier);
    }

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(SpannerClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(SpannerConfig.class);
        configBinder(binder).bindConfig(JdbcStatisticsConfig.class);
        newSetBinder(binder, TablePropertiesProvider.class).addBinding().to(SpannerTableProperties.class);
        install(new DecimalModule());
        install(new JdbcJoinPushdownSupportModule());
        install(new RemoteQueryCancellationModule());
        newSetBinder(binder, ConnectorTableFunction.class).addBinding().toProvider(Query.class).in(Scopes.SINGLETON);
    }
}
