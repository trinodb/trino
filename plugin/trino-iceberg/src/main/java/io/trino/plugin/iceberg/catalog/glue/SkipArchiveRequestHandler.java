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
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;

public class SkipArchiveRequestHandler
        extends RequestHandler2
{
    private final boolean skipArchive;

    public SkipArchiveRequestHandler(boolean skipArchive)
    {
        this.skipArchive = skipArchive;
    }

    @Override
    public AmazonWebServiceRequest beforeExecution(AmazonWebServiceRequest request)
    {
        if (request instanceof UpdateTableRequest updateTableRequest) {
            return updateTableRequest.withSkipArchive(skipArchive);
        }
        if (request instanceof CreateDatabaseRequest ||
                request instanceof DeleteDatabaseRequest ||
                request instanceof GetDatabasesRequest ||
                request instanceof GetDatabaseRequest ||
                request instanceof CreateTableRequest ||
                request instanceof DeleteTableRequest ||
                request instanceof GetTablesRequest ||
                request instanceof GetTableRequest) {
            return request;
        }
        throw new IllegalArgumentException("Unsupported request: " + request);
    }
}
