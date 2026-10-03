package tech.ydb.trino;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.util.Modules;
import io.trino.plugin.base.mapping.IdentifierMapping;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.QueryBuilder;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;

public class TestingYdbJdbcModule extends AbstractModule {

    @Override
    protected void configure() {
        install(Modules.override(new YdbClientModule()).with(new AbstractModule() {
            @Provides
            @Singleton
            @ForBaseJdbc
            public JdbcClient provideJdbcClient(
                    BaseJdbcConfig config,
                    ConnectionFactory connectionFactory,
                    QueryBuilder queryBuilder,
                    IdentifierMapping identifierMapping,
                    RemoteQueryModifier remoteQueryModifier) {
                return new TestingYdbJdbcClient(config, connectionFactory, queryBuilder, identifierMapping, remoteQueryModifier);
            }
        }));
    }
}
