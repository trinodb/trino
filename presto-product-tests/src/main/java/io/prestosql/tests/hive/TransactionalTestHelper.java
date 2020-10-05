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
package io.prestosql.tests.hive;

import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.prestosql.plugin.hive.metastore.thrift.NoHiveMetastoreAuthentication;
import io.prestosql.plugin.hive.metastore.thrift.ThriftHiveMetastoreClient;
import io.prestosql.plugin.hive.metastore.thrift.Transport;
import io.prestosql.tempto.hadoop.hdfs.HdfsClient;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.thrift.TException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Optional;

public final class TransactionalTestHelper
{
    private static final String LOCALHOST = "localhost";
    private static final String DEFAULT_FS = "hdfs://hadoop-master:9000";

    @Inject
    private HdfsClient hdfsClient;

    @Inject
    @Named("databases.hive.warehouse_directory_path")
    private String warehouseDirectory;

    @Inject
    @Named("databases.hive.metastore.host")
    private String metastoreHost;

    @Inject
    @Named("databases.hive.metastore.port")
    private int metastorePort;

    // Simulates an aborted transaction which leaves behind a file in a table partition with some data
    public void simulateAbortedHiveTransaction(String database, String tableName)
            throws TException, IOException
    {
        ThriftHiveMetastoreClient client = createMetastoreClient();
        try {
            long transaction = client.openTransaction("test");

            client.allocateTableWriteIds(database, tableName, Collections.singletonList(transaction)).get(0).getWriteId();

            // Rollback transaction which leaves behind a delta directory deltaC i.e. 'delta_0000003_0000003_0000'
            client.abortTransaction(transaction);

            String deltaA = warehouseDirectory + "/" + tableName + "/delta_0000001_0000001_0000/bucket_00000";
            String deltaB = warehouseDirectory + "/" + tableName + "/delta_0000002_0000002_0000/bucket_00000";
            String deltaC = warehouseDirectory + "/" + tableName + "/delta_0000003_0000003_0000/bucket_00000";

            System.setProperty("HADOOP_USER_NAME", "hdfs");
            // Delete original delta B, C
            hdfsClient.delete(deltaB);
            hdfsClient.delete(deltaC);

            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            hdfsClient.loadFile(deltaA, byteArrayOutputStream);

            // Copy content of delta A to delta B
            hdfsClient.saveFile(deltaB, new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));

            // Copy content of delta A to delta C (which is an aborted transaction)
            hdfsClient.saveFile(deltaC, new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        }
        finally {
            client.close();
        }
    }

    public LockResponse acquireDropTableLock(ThriftHiveMetastoreClient client, String database, String tableName, long transaction)
            throws TException
    {
        AllocateTableWriteIdsRequest rqst = new AllocateTableWriteIdsRequest(database, tableName);
        rqst.setTxnIds(Collections.singletonList(transaction));

        LockComponentBuilder builder = new LockComponentBuilder();
        builder.setExclusive();
        builder.setOperationType(DataOperationType.NO_TXN);

        builder.setDbName(database);
        builder.setTableName(tableName);

        // acquire locks is called only for TransactionalTable
        builder.setIsTransactional(true);

        LockRequestBuilder request = new LockRequestBuilder()
                .setTransactionId(transaction)
                .setUser("hdfs");
        request.addLockComponent(builder.build());
        LockRequest lockRequest = request.build();
        return client.acquireLock(lockRequest);
    }

    public ThriftHiveMetastoreClient createMetastoreClient()
            throws TException
    {
        URI metastore = URI.create("thrift://" + metastoreHost + ":" + metastorePort);
        return new ThriftHiveMetastoreClient(
                Transport.create(
                        HostAndPort.fromParts(metastore.getHost(), metastore.getPort()),
                        Optional.empty(),
                        Optional.empty(),
                        10000,
                        new NoHiveMetastoreAuthentication(),
                        Optional.empty()), LOCALHOST);
    }
}
