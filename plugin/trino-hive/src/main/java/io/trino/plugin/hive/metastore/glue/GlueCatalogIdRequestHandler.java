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

import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

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
        SdkRequest request = context.request();
        if (request instanceof GetDatabasesRequest) {
            return ((GetDatabasesRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof GetDatabaseRequest) {
            return ((GetDatabaseRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof CreateDatabaseRequest) {
            return ((CreateDatabaseRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof UpdateDatabaseRequest) {
            return ((UpdateDatabaseRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof DeleteDatabaseRequest) {
            return ((DeleteDatabaseRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof GetTablesRequest) {
            return ((GetTablesRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof GetTableRequest) {
            return ((GetTableRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof CreateTableRequest) {
            return ((CreateTableRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof UpdateTableRequest) {
            return ((UpdateTableRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof DeleteTableRequest) {
            return ((DeleteTableRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof GetPartitionsRequest) {
            return ((GetPartitionsRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof GetPartitionRequest) {
            return ((GetPartitionRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof UpdatePartitionRequest) {
            return ((UpdatePartitionRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof DeletePartitionRequest) {
            return ((DeletePartitionRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof BatchGetPartitionRequest) {
            return ((BatchGetPartitionRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof BatchCreatePartitionRequest) {
            return ((BatchCreatePartitionRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof BatchUpdatePartitionRequest) {
            return ((BatchUpdatePartitionRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof GetColumnStatisticsForTableRequest) {
            return ((GetColumnStatisticsForTableRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof UpdateColumnStatisticsForTableRequest) {
            return ((UpdateColumnStatisticsForTableRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof DeleteColumnStatisticsForTableRequest) {
            return ((DeleteColumnStatisticsForTableRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof GetColumnStatisticsForPartitionRequest) {
            return ((GetColumnStatisticsForPartitionRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof UpdateColumnStatisticsForPartitionRequest) {
            return ((UpdateColumnStatisticsForPartitionRequest) request).toBuilder().catalogId(catalogId).build();
        }
        if (request instanceof DeleteColumnStatisticsForPartitionRequest) {
            return ((DeleteColumnStatisticsForPartitionRequest) request).toBuilder().catalogId(catalogId).build();
        }
        throw new IllegalArgumentException("Unsupported request: " + request);
    }
}
