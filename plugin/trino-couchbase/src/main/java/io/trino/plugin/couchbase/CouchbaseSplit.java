package io.trino.plugin.couchbase;

import io.trino.spi.connector.ConnectorSplit;

public record CouchbaseSplit (
            ) implements ConnectorSplit {

}
