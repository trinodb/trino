package io.trino.loki;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunction;

import static java.util.Objects.requireNonNull;

public class LokiTableFunctionProvider
        implements Provider<ConnectorTableFunction> {
    //public static final String SCHEMA_NAME = "system";
    //public static final String NAME = "query";

    //private final LokiMetadata lokiMetadata;

    @Inject
    public LokiTableFunctionProvider(LokiMetadata lokiMetadata) {
        //this.lokiMetadata = requireNonNull(lokiMetadata, "cassandraMetadata is null");
    }

    @Override
    public ConnectorTableFunction get() {
        return new ClassLoaderSafeConnectorTableFunction(new LokiTableFunction(), getClass().getClassLoader());
    }
}
