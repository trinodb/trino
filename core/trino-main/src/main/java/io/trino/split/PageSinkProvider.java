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
package io.trino.split;

import io.trino.Session;
import io.trino.metadata.InsertTableHandle;
import io.trino.metadata.MergeHandle;
import io.trino.metadata.OutputTableHandle;
import io.trino.metadata.TableExecuteHandle;
import io.trino.spi.connector.ConnectorMergeSink;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;

public interface PageSinkProvider
{
    /*
     * Used for CTAS
     */
    ConnectorPageSink createPageSink(Session session, OutputTableHandle tableHandle, ConnectorPageSinkId pageSinkId);

    /*
     * Used to insert into an existing table
     */
    ConnectorPageSink createPageSink(Session session, InsertTableHandle tableHandle, ConnectorPageSinkId pageSinkId);

    ConnectorPageSink createPageSink(Session session, TableExecuteHandle tableHandle, ConnectorPageSinkId pageSinkId);

    /*
     * Used to write the result of SQL MERGE to an existing table
     */
    ConnectorMergeSink createMergeSink(Session session, MergeHandle mergeHandle, ConnectorPageSinkId pageSinkId);
}
