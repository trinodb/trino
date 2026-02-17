package io.trino.plugin.couchbase;

import com.couchbase.client.java.manager.collection.CollectionManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;

import java.io.File;
import java.util.Collections;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class CouchbaseMetadata
                implements ConnectorMetadata {

    private final TypeManager typeManager;
    private final CouchbaseClient client;
    private final CouchbaseConfig config;

    @Inject
    public CouchbaseMetadata(TypeManager typeManager, CouchbaseClient client, CouchbaseConfig config)
    {
        this.typeManager = typeManager;
        this.client = client;
        this.config = config;
    }
    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName) {
        return client.getBucket().collections().getAllScopes().stream()
                .filter(scope -> scope.name().equals(schemaName))
                .findAny().isPresent();
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion) {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        CollectionManager cm = client.getBucket().collections();
        return cm.getAllScopes().stream()
                .filter(scopeSpec -> scopeSpec.name().equals(tableName.getSchemaName()))
                .flatMap(scopeSpec -> scopeSpec.collections().stream())
                .filter(collectionSpec -> collectionSpec.name().equals(tableName.getTableName()))
                .findFirst().map(collectionSpec -> new CouchbaseTableHandle(tableName.getSchemaName(), tableName.getTableName(), Collections.EMPTY_SET))
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        requireNonNull(config.getSchemaFolder(), "Couchbase schema folder is not set");
        if (!(table instanceof CouchbaseTableHandle)) {
            throw new RuntimeException("Couchbase table handle is not an instance of CouchbaseTableHandle");
        }

        CouchbaseTableHandle handle = (CouchbaseTableHandle) table;
        File schemaFile = new File(new File(config.getSchemaFolder()), String.format("%s.%s.%s.json", client.getBucket().name(), handle.schema(), handle.name()));
        if (!schemaFile.exists()) {
            throw new RuntimeException(String.format("Couchbase schema file '%s' does not exist", schemaFile.getAbsolutePath()));
        }
        return null;
    }
}
