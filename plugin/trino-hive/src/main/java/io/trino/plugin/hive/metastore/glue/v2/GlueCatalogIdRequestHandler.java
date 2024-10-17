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
package io.trino.plugin.hive.metastore.glue.v2;

import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.DeleteColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsRequest;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionRequest;

import static java.util.Objects.requireNonNull;

public class GlueCatalogIdRequestHandler
        implements ExecutionInterceptor
{
    private final String catalogId;

    public GlueCatalogIdRequestHandler(String catalogId)
    {
        this.catalogId = requireNonNull(catalogId, "catalogId is null");
    }

    @Override
    public SdkRequest modifyRequest(Context.ModifyRequest context, ExecutionAttributes executionAttributes)
    {
        return switch (context.request()) {
            case GetDatabasesRequest request -> request.toBuilder().catalogId(catalogId).build();
            case GetDatabaseRequest request -> request.toBuilder().catalogId(catalogId).build();
            case CreateDatabaseRequest request -> request.toBuilder().catalogId(catalogId).build();
            case UpdateDatabaseRequest request -> request.toBuilder().catalogId(catalogId).build();
            case DeleteDatabaseRequest request -> request.toBuilder().catalogId(catalogId).build();
            case GetTablesRequest request -> request.toBuilder().catalogId(catalogId).build();
            case GetTableRequest request -> request.toBuilder().catalogId(catalogId).build();
            case CreateTableRequest request -> request.toBuilder().catalogId(catalogId).build();
            case UpdateTableRequest request -> request.toBuilder().catalogId(catalogId).build();
            case DeleteTableRequest request -> request.toBuilder().catalogId(catalogId).build();
            case GetPartitionsRequest request -> request.toBuilder().catalogId(catalogId).build();
            case GetPartitionRequest request -> request.toBuilder().catalogId(catalogId).build();
            case UpdatePartitionRequest request -> request.toBuilder().catalogId(catalogId).build();
            case DeletePartitionRequest request -> request.toBuilder().catalogId(catalogId).build();
            case BatchGetPartitionRequest request -> request.toBuilder().catalogId(catalogId).build();
            case BatchCreatePartitionRequest request -> request.toBuilder().catalogId(catalogId).build();
            case BatchUpdatePartitionRequest request -> request.toBuilder().catalogId(catalogId).build();
            case GetColumnStatisticsForTableRequest request -> request.toBuilder().catalogId(catalogId).build();
            case UpdateColumnStatisticsForTableRequest request -> request.toBuilder().catalogId(catalogId).build();
            case DeleteColumnStatisticsForTableRequest request -> request.toBuilder().catalogId(catalogId).build();
            case GetColumnStatisticsForPartitionRequest request -> request.toBuilder().catalogId(catalogId).build();
            case UpdateColumnStatisticsForPartitionRequest request -> request.toBuilder().catalogId(catalogId).build();
            case DeleteColumnStatisticsForPartitionRequest request -> request.toBuilder().catalogId(catalogId).build();
            case GetUserDefinedFunctionsRequest request -> request.toBuilder().catalogId(catalogId).build();
            case GetUserDefinedFunctionRequest request -> request.toBuilder().catalogId(catalogId).build();
            case CreateUserDefinedFunctionRequest request -> request.toBuilder().catalogId(catalogId).build();
            case UpdateUserDefinedFunctionRequest request -> request.toBuilder().catalogId(catalogId).build();
            case DeleteUserDefinedFunctionRequest request -> request.toBuilder().catalogId(catalogId).build();
            default -> throw new IllegalStateException("Unexpected value: " + context.request());
        };
    }
}
