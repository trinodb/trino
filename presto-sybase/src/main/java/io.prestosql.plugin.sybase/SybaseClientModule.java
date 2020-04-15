package io.prestosql.plugin.sybase;

import com.sybase.jdbc4.jdbc.SybDriver;
import io.prestosql.plugin.jdbc.*;
import com.google.inject.*;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import static io.airlift.configuration.ConfigBinder.configBinder;

/**
 * Guice Implementation to create correct DI and binds
 */

 public class SybaseClientModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(Key.get(JdbcClient.class, ForBaseJdbc.class))
            .to(SybaseClient.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(TypeHandlingJdbcConfig.class);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public ConnectionFactory createConnectionFactory(BaseJdbcConfig baseJdbcConfig, CredentialProvider credentialProvider) {
        return new DriverConnectionFactory(new SybDriver(), baseJdbcConfig, credentialProvider);
    }
 }
