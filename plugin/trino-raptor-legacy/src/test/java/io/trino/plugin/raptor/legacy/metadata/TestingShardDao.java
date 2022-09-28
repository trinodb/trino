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
package io.trino.plugin.raptor.legacy.metadata;

import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.Set;
import java.util.UUID;

@RegisterConstructorMapper(ShardNode.class)
interface TestingShardDao
        extends H2ShardDao
{
    @SqlQuery("SELECT shard_uuid FROM shards WHERE table_id = :tableId")
    Set<UUID> getShards(long tableId);

    @SqlQuery("SELECT s.shard_uuid, n.node_identifier\n" +
            "FROM shards s\n" +
            "JOIN shard_nodes sn ON (s.shard_id = sn.shard_id)\n" +
            "JOIN nodes n ON (sn.node_id = n.node_id)\n" +
            "WHERE s.table_id = :tableId")
    Set<ShardNode> getShardNodes(long tableId);

    @SqlQuery("SELECT node_identifier FROM nodes")
    Set<String> getAllNodesInUse();

    @SqlUpdate("INSERT INTO shards (shard_uuid, table_id, bucket_number, create_time, row_count, compressed_size, uncompressed_size, xxhash64)\n" +
            "VALUES (:shardUuid, :tableId, :bucketNumber, CURRENT_TIMESTAMP, :rowCount, :compressedSize, :uncompressedSize, :xxhash64)")
    @GetGeneratedKeys
    long insertShard(
            UUID shardUuid,
            long tableId,
            Integer bucketNumber,
            long rowCount,
            long compressedSize,
            long uncompressedSize,
            long xxhash64);

    @SqlUpdate("INSERT INTO shard_nodes (shard_id, node_id)\n" +
            "VALUES (:shardId, :nodeId)\n")
    void insertShardNode(long shardId, int nodeId);
}
