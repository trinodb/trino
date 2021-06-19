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
package io.trino.plugin.thrift.api;

import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.TException;
import io.airlift.drift.annotations.ThriftException;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftMethod;
import io.airlift.drift.annotations.ThriftService;

import java.util.List;

/**
 * Trino Thrift service definition.
 * This thrift service needs to be implemented in order to be used with Thrift Connector.
 */
@ThriftService(value = "trino", idlName = "TrinoThriftService")
public interface TrinoThriftService
{
    /**
     * Returns available schema names.
     */
    @ThriftMethod("trinoListSchemaNames")
    List<String> listSchemaNames()
            throws TrinoThriftServiceException, TException;

    /**
     * Returns tables for the given schema name.
     *
     * @param schemaNameOrNull a structure containing schema name or {@literal null}
     * @return a list of table names with corresponding schemas. If schema name is null then returns
     * a list of tables for all schemas. Returns an empty list if a schema does not exist
     */
    @ThriftMethod("trinoListTables")
    List<TrinoThriftSchemaTableName> listTables(
            @ThriftField(name = "schemaNameOrNull") TrinoThriftNullableSchemaName schemaNameOrNull)
            throws TrinoThriftServiceException, TException;

    /**
     * Returns metadata for a given table.
     *
     * @param schemaTableName schema and table name
     * @return metadata for a given table, or a {@literal null} value inside if it does not exist
     */
    @ThriftMethod("trinoGetTableMetadata")
    TrinoThriftNullableTableMetadata getTableMetadata(
            @ThriftField(name = "schemaTableName") TrinoThriftSchemaTableName schemaTableName)
            throws TrinoThriftServiceException, TException;

    /**
     * Returns a batch of splits.
     *
     * @param schemaTableName schema and table name
     * @param desiredColumns a superset of columns to return; empty set means "no columns", {@literal null} set means "all columns"
     * @param outputConstraint constraint on the returned data
     * @param maxSplitCount maximum number of splits to return
     * @param nextToken token from a previous split batch or {@literal null} if it is the first call
     * @return a batch of splits
     */
    @ThriftMethod(value = "trinoGetSplits",
            exception = @ThriftException(type = TrinoThriftServiceException.class, id = 1))
    ListenableFuture<TrinoThriftSplitBatch> getSplits(
            @ThriftField(name = "schemaTableName") TrinoThriftSchemaTableName schemaTableName,
            @ThriftField(name = "desiredColumns") TrinoThriftNullableColumnSet desiredColumns,
            @ThriftField(name = "outputConstraint") TrinoThriftTupleDomain outputConstraint,
            @ThriftField(name = "maxSplitCount") int maxSplitCount,
            @ThriftField(name = "nextToken") TrinoThriftNullableToken nextToken);

    /**
     * Returns a batch of index splits for the given batch of keys.
     * This method is called if index join strategy is chosen for a query.
     *
     * @param schemaTableName schema and table name
     * @param indexColumnNames specifies columns and their order for keys
     * @param outputColumnNames a list of column names to return
     * @param keys keys for which records need to be returned; includes only unique and non-null values
     * @param outputConstraint constraint on the returned data
     * @param maxSplitCount maximum number of splits to return
     * @param nextToken token from a previous split batch or {@literal null} if it is the first call
     * @return a batch of splits
     */
    @ThriftMethod(value = "trinoGetIndexSplits",
            exception = @ThriftException(type = TrinoThriftServiceException.class, id = 1))
    ListenableFuture<TrinoThriftSplitBatch> getIndexSplits(
            @ThriftField(name = "schemaTableName") TrinoThriftSchemaTableName schemaTableName,
            @ThriftField(name = "indexColumnNames") List<String> indexColumnNames,
            @ThriftField(name = "outputColumnNames") List<String> outputColumnNames,
            @ThriftField(name = "keys") TrinoThriftPageResult keys,
            @ThriftField(name = "outputConstraint") TrinoThriftTupleDomain outputConstraint,
            @ThriftField(name = "maxSplitCount") int maxSplitCount,
            @ThriftField(name = "nextToken") TrinoThriftNullableToken nextToken);

    /**
     * Returns a batch of rows for the given split.
     *
     * @param splitId split id as returned in split batch
     * @param columns a list of column names to return
     * @param maxBytes maximum size of returned data in bytes
     * @param nextToken token from a previous batch or {@literal null} if it is the first call
     * @return a batch of table data
     */
    @ThriftMethod(value = "trinoGetRows",
            exception = @ThriftException(type = TrinoThriftServiceException.class, id = 1))
    ListenableFuture<TrinoThriftPageResult> getRows(
            @ThriftField(name = "splitId") TrinoThriftId splitId,
            @ThriftField(name = "columns") List<String> columns,
            @ThriftField(name = "maxBytes") long maxBytes,
            @ThriftField(name = "nextToken") TrinoThriftNullableToken nextToken);
}
