package io.trino.loki;

import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.spi.connector.Connector;

import static java.util.Objects.requireNonNull;

public class LokiConnector implements Connector {
    private static final Logger log = Logger.get(LokiConnector.class);

    private final LokiMetadata metadata;
    private final LokiSplitManager splitManager;
    private final LokiRecordSetProvider recordSetProvider;

    @Inject
    public LokiConnector(
            LokiMetadata metadata,
            LokiSplitManager splitManager,
            LokiRecordSetProvider recordSetProvider)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.recordSetProvider = requireNonNull(recordSetProvider, "recordSetProvider is null");
    }
}
