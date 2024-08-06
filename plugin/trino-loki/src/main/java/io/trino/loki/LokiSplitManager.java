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
package io.trino.loki;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.spi.connector.*;

import java.util.List;

public class LokiSplitManager
        implements ConnectorSplitManager
{

    private static final Logger log = Logger.get(LokiSplitManager.class);

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {

        var table = (LokiTableHandle) connectorTableHandle;

        // TODO: support multiple splits by splitting on time.
        List<ConnectorSplit> splits = ImmutableList.of(new LokiSplit(table.query(), table.start(), table.end()));

        log.debug("created %d splits", splits.size());
        return new FixedSplitSource(splits);
    }
}
