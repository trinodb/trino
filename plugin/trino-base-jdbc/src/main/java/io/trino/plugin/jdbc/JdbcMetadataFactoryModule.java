package io.trino.plugin.jdbc;

import com.google.inject.AbstractModule;
import com.google.inject.assistedinject.FactoryModuleBuilder;

public class JdbcMetadataFactoryModule
        extends AbstractModule
{
    @Override
    public void configure()
    {
        install(new FactoryModuleBuilder()
                .implement(JdbcMetadata.class, DefaultJdbcMetadata.class)
                .build(JdbcMetadataFactory.class));
    }
}
