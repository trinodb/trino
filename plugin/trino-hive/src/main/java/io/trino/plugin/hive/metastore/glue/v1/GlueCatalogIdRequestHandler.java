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
package io.trino.plugin.hive.metastore.glue.v1;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.handlers.RequestHandler2;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchGetPartitionRequest;
import com.amazonaws.services.glue.model.BatchUpdatePartitionRequest;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.DeleteUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsRequest;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.amazonaws.services.glue.model.UpdateUserDefinedFunctionRequest;

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
    public AmazonWebServiceRequest beforeExecution(AmazonWebServiceRequest serviceRequest)
    {
        return switch (serviceRequest) {
            case GetDatabasesRequest request -> request.withCatalogId(catalogId);
            case GetDatabaseRequest request -> request.withCatalogId(catalogId);
            case CreateDatabaseRequest request -> request.withCatalogId(catalogId);
            case UpdateDatabaseRequest request -> request.withCatalogId(catalogId);
            case DeleteDatabaseRequest request -> request.withCatalogId(catalogId);
            case GetTablesRequest request -> request.withCatalogId(catalogId);
            case GetTableRequest request -> request.withCatalogId(catalogId);
            case CreateTableRequest request -> request.withCatalogId(catalogId);
            case UpdateTableRequest request -> request.withCatalogId(catalogId);
            case DeleteTableRequest request -> request.withCatalogId(catalogId);
            case GetPartitionsRequest request -> request.withCatalogId(catalogId);
            case GetPartitionRequest request -> request.withCatalogId(catalogId);
            case UpdatePartitionRequest request -> request.withCatalogId(catalogId);
            case DeletePartitionRequest request -> request.withCatalogId(catalogId);
            case BatchGetPartitionRequest request -> request.withCatalogId(catalogId);
            case BatchCreatePartitionRequest request -> request.withCatalogId(catalogId);
            case BatchUpdatePartitionRequest request -> request.withCatalogId(catalogId);
            case GetColumnStatisticsForTableRequest request -> request.withCatalogId(catalogId);
            case UpdateColumnStatisticsForTableRequest request -> request.withCatalogId(catalogId);
            case DeleteColumnStatisticsForTableRequest request -> request.withCatalogId(catalogId);
            case GetColumnStatisticsForPartitionRequest request -> request.withCatalogId(catalogId);
            case UpdateColumnStatisticsForPartitionRequest request -> request.withCatalogId(catalogId);
            case DeleteColumnStatisticsForPartitionRequest request -> request.withCatalogId(catalogId);
            case GetUserDefinedFunctionsRequest request -> request.withCatalogId(catalogId);
            case GetUserDefinedFunctionRequest request -> request.withCatalogId(catalogId);
            case CreateUserDefinedFunctionRequest request -> request.withCatalogId(catalogId);
            case UpdateUserDefinedFunctionRequest request -> request.withCatalogId(catalogId);
            case DeleteUserDefinedFunctionRequest request -> request.withCatalogId(catalogId);
            default -> throw new IllegalArgumentException("Unsupported request: " + serviceRequest);
        };
    }
}
