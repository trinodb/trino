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
package com.aliyun.odps.cupid.trino;

import com.aliyun.odps.cupid.table.v1.Attribute;
import com.aliyun.odps.cupid.table.v1.reader.*;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.*;
import io.trino.spi.predicate.NullableValue;
import org.apache.commons.codec.binary.Base64;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class OdpsSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final OdpsClient odpsClient;
    private final String tableApiProvider;
    private final int splitSize;

    @Inject
    public OdpsSplitManager(OdpsConnectorId connectorId, OdpsClient odpsClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.odpsClient = requireNonNull(odpsClient, "client is null");
        if (System.getenv("META_LOOKUP_NAME") != null) {
            tableApiProvider = "cupid-native";
        } else {
            tableApiProvider = "tunnel";
        }
        splitSize = odpsClient.getOdpsConfig().getSplitSize() <= 0 ? 256 : odpsClient.getOdpsConfig().getSplitSize();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        OdpsTableHandle handle = (OdpsTableHandle) tableHandle;
        OdpsTable table = odpsClient.getTable(handle.getSchemaName(),
                handle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists",
                handle.getSchemaName(), handle.getTableName());

        List<OdpsPartition> partitions = odpsClient.getOdpsPartitions(handle.getSchemaName(),
                handle.getTableName(),
                handle.getOdpsTable(),
                constraint);
        List<OdpsColumnHandle> desiredColumns = handle.getDesiredColumns().size() > 0 ? handle.getDesiredColumns() : table.getDataColumns();
        List<ConnectorSplit> splits = new ArrayList<>();
        TableReadSession tableReadSession;
        InputSplit[] inputSplits;
        boolean isZeroColumn = false;
        try {
            Set<String> partitionColumnNames = handle.getOdpsTable().getPartitionColumns().stream().map(e -> e.getName()).collect(Collectors.toSet());
            List<Attribute> reqColumns = desiredColumns.stream()
                    .filter(e -> !partitionColumnNames.contains(e.getName()))
                    .map(e -> new Attribute(e.getName(), OdpsUtils.toOdpsType(e.getType(), e.getIsStringType()).getTypeName())).collect(Collectors.toList());
            if (reqColumns.size() == 0) {
                // tunnel must set columns
                OdpsColumnHandle columnHandle = handle.getOdpsTable().getDataColumns().get(0);
                reqColumns.add(0, new Attribute(columnHandle.getName(),
                        OdpsUtils.toOdpsType(columnHandle.getType(), columnHandle.getIsStringType()).getTypeName()));
                isZeroColumn = true;
            }
            RequiredSchema requiredSchema = RequiredSchema.columns(reqColumns);
            if (table.getPartitionColumns().size() > 0) {
                List<PartitionSpecWithBucketFilter> partitionSpecWithBucketFilterList = partitions.stream()
                        .map(e -> new PartitionSpecWithBucketFilter(getPartitionSpecKVMap(e.getKeys())))
                        .collect(Collectors.toList());
                if (partitionSpecWithBucketFilterList.size() == 0) {
                    // no part specified
                    return new FixedSplitSource(ImmutableList.of());
                }
                tableReadSession = new TableReadSessionBuilder(tableApiProvider, handle.getSchemaName(),
                        handle.getTableName())
                        .readPartitions(partitionSpecWithBucketFilterList)
                        .readDataColumns(requiredSchema)
                        .build();
            } else {
                tableReadSession = new TableReadSessionBuilder(tableApiProvider, handle.getSchemaName(),
                        handle.getTableName())
                        .readDataColumns(requiredSchema)
                        .build();
            }
            inputSplits = tableReadSession.getOrCreateInputSplits(splitSize);
        } catch (Exception e) {
            throw new TrinoException(OdpsErrorCode.ODPS_INTERNAL_ERROR, "create TableReadSession failed!" + e.toString(), e);
        }

        for (InputSplit inputSplit : inputSplits) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try {
                ObjectOutputStream out = new ObjectOutputStream(baos);
                out.writeObject(inputSplit);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            String splitBase64Str = Base64.encodeBase64String(baos.toByteArray());
            splits.add(new OdpsSplit(connectorId, handle.getSchemaName(),
                    handle.getTableName(), splitBase64Str, isZeroColumn));
        }
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }

    private Map<String, String> getPartitionSpecKVMap(Map<ColumnHandle, NullableValue> kvs) {
        Map<String, String> parts = new LinkedHashMap<>(2);
        for (Map.Entry<ColumnHandle, NullableValue> kv : kvs.entrySet()) {
            parts.put(((OdpsColumnHandle) kv.getKey()).getName(), ((Slice) kv.getValue().getValue()).toStringUtf8());
        }

        return parts;
    }
}
