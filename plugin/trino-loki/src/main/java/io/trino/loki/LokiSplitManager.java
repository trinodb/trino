package io.trino.loki;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.*;

import java.util.List;

public class LokiSplitManager implements ConnectorSplitManager {

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint) {

        // TODO: support multiple splits by splitting on time.
        List<ConnectorSplit> splits = ImmutableList.of(new LokiSplit());
        return new FixedSplitSource(splits);
    }
}
