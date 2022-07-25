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
package io.trino.plugin.raptor.legacy.storage.organization;

import org.jdbi.v3.sqlobject.config.RegisterConstructorMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.Set;

@RegisterConstructorMapper(TableOrganizationInfo.class)
public interface ShardOrganizerDao
{
    @SqlUpdate("INSERT INTO shard_organizer_jobs (node_identifier, table_id, last_start_time)\n" +
            "VALUES (:nodeIdentifier, :tableId, NULL)")
    void insertNode(String nodeIdentifier, long tableId);

    @SqlUpdate("UPDATE shard_organizer_jobs SET last_start_time = :lastStartTime\n" +
            "   WHERE node_identifier = :nodeIdentifier\n" +
            "     AND table_id = :tableId")
    void updateLastStartTime(
            String nodeIdentifier,
            long tableId,
            long lastStartTime);

    @SqlQuery("SELECT table_id, last_start_time\n" +
            "   FROM shard_organizer_jobs\n" +
            "   WHERE node_identifier = :nodeIdentifier")
    Set<TableOrganizationInfo> getNodeTableOrganizationInfo(String nodeIdentifier);

    @SqlUpdate("DELETE FROM shard_organizer_jobs WHERE table_id = :tableId")
    void dropOrganizerJobs(long tableId);
}
