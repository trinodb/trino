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
package io.trino.plugin.hive.metastore.glue;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.BatchUpdatePartitionRequest;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;

import static java.util.Objects.requireNonNull;

public class GlueCatalogIdRequestHandler
        extends RequestHandler2
{
    private final String catalogId;

    public GlueCatalogIdRequestHandler(String catalogId)
    {
        this.catalogId = requireNonNull(catalogId, "catalogId is null");
    }

    @Override
    public AmazonWebServiceRequest beforeExecution(AmazonWebServiceRequest request)
    {
        if (request instanceof GetDatabasesRequest) {
            return ((GetDatabasesRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof GetDatabaseRequest) {
            return ((GetDatabaseRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof CreateDatabaseRequest) {
            return ((CreateDatabaseRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof UpdateDatabaseRequest) {
            return ((UpdateDatabaseRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof DeleteDatabaseRequest) {
            return ((DeleteDatabaseRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof GetTablesRequest) {
            return ((GetTablesRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof GetTableRequest) {
            return ((GetTableRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof CreateTableRequest) {
            return ((CreateTableRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof UpdateTableRequest) {
            return ((UpdateTableRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof DeleteTableRequest) {
            return ((DeleteTableRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof GetPartitionsRequest) {
            return ((GetPartitionsRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof GetPartitionRequest) {
            return ((GetPartitionRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof UpdatePartitionRequest) {
            return ((UpdatePartitionRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof DeletePartitionRequest) {
            return ((DeletePartitionRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof BatchGetPartitionRequest) {
            return ((BatchGetPartitionRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof BatchCreatePartitionRequest) {
            return ((BatchCreatePartitionRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof BatchUpdatePartitionRequest) {
            return ((BatchUpdatePartitionRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof GetColumnStatisticsForTableRequest) {
            return ((GetColumnStatisticsForTableRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof UpdateColumnStatisticsForTableRequest) {
            return ((UpdateColumnStatisticsForTableRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof DeleteColumnStatisticsForTableRequest) {
            return ((DeleteColumnStatisticsForTableRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof GetColumnStatisticsForPartitionRequest) {
            return ((GetColumnStatisticsForPartitionRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof UpdateColumnStatisticsForPartitionRequest) {
            return ((UpdateColumnStatisticsForPartitionRequest) request).withCatalogId(catalogId);
        }
        if (request instanceof DeleteColumnStatisticsForPartitionRequest) {
            return ((DeleteColumnStatisticsForPartitionRequest) request).withCatalogId(catalogId);
        }
        throw new IllegalArgumentException("Unsupported request: " + request);
    }
}
