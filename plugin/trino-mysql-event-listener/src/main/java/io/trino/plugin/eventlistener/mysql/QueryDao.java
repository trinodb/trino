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
package io.trino.plugin.eventlistener.mysql;

import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public interface QueryDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS trino_queries (\n" +
            "  query_id VARCHAR(255) NOT NULL PRIMARY KEY,\n" +
            "  transaction_id VARCHAR(255) NULL,\n" +
            "  query MEDIUMTEXT NOT NULL,\n" +
            "  update_type VARCHAR(255) NULL,\n" +
            "  prepared_query MEDIUMTEXT NULL,\n" +
            "  query_state VARCHAR(255) NOT NULL,\n" +
            "  plan MEDIUMTEXT NULL,\n" +
            "  stage_info_json MEDIUMTEXT NULL,\n" +
            "  user VARCHAR(255) NOT NULL,\n" +
            "  principal VARCHAR(255) NULL,\n" +
            "  trace_token VARCHAR(255) NULL,\n" +
            "  remote_client_address VARCHAR(255) NULL,\n" +
            "  user_agent VARCHAR(255) NULL,\n" +
            "  client_info VARCHAR(255) NULL,\n" +
            "  client_tags_json MEDIUMTEXT NOT NULL,\n" +
            "  source VARCHAR(255) NULL,\n" +
            "  catalog VARCHAR(255) NULL,\n" +
            "  `schema` VARCHAR(255) NULL,\n" +
            "  resource_group_id VARCHAR(255) NULL,\n" +
            "  session_properties_json MEDIUMTEXT NOT NULL,\n" +
            "  server_address VARCHAR(255) NOT NULL,\n" +
            "  server_version VARCHAR(255) NOT NULL,\n" +
            "  environment VARCHAR(255) NOT NULL,\n" +
            "  query_type VARCHAR(255) NULL,\n" +
            "  inputs_json MEDIUMTEXT NOT NULL,\n" +
            "  output_json MEDIUMTEXT NULL,\n" +
            "  error_code VARCHAR(255) NULL,\n" +
            "  error_type VARCHAR(255) NULL,\n" +
            "  failure_type VARCHAR(255) NULL,\n" +
            "  failure_message MEDIUMTEXT NULL,\n" +
            "  failure_task VARCHAR(255) NULL,\n" +
            "  failure_host VARCHAR(255) NULL,\n" +
            "  failures_json MEDIUMTEXT NULL,\n" +
            "  warnings_json MEDIUMTEXT NOT NULL,\n" +
            "  cpu_time_millis BIGINT NOT NULL,\n" +
            "  failed_cpu_time_millis BIGINT NOT NULL,\n" +
            "  wall_time_millis BIGINT NOT NULL,\n" +
            "  queued_time_millis BIGINT NOT NULL,\n" +
            "  scheduled_time_millis BIGINT NOT NULL,\n" +
            "  failed_scheduled_time_millis BIGINT NOT NULL,\n" +
            "  waiting_time_millis BIGINT NOT NULL,\n" +
            "  analysis_time_millis BIGINT NOT NULL,\n" +
            "  planning_time_millis BIGINT NOT NULL,\n" +
            "  execution_time_millis BIGINT NOT NULL,\n" +
            "  input_blocked_time_millis BIGINT NOT NULL,\n" +
            "  failed_input_blocked_time_millis BIGINT NOT NULL,\n" +
            "  output_blocked_time_millis BIGINT NOT NULL,\n" +
            "  failed_output_blocked_time_millis BIGINT NOT NULL,\n" +
            "  physical_input_read_time_millis BIGINT NOT NULL,\n" +
            "  peak_memory_bytes BIGINT NOT NULL,\n" +
            "  peak_task_memory_bytes BIGINT NOT NULL,\n" +
            "  physical_input_bytes BIGINT NOT NULL,\n" +
            "  physical_input_rows BIGINT NOT NULL,\n" +
            "  internal_network_bytes BIGINT NOT NULL,\n" +
            "  internal_network_rows BIGINT NOT NULL,\n" +
            "  total_bytes BIGINT NOT NULL,\n" +
            "  total_rows BIGINT NOT NULL,\n" +
            "  output_bytes BIGINT NOT NULL,\n" +
            "  output_rows BIGINT NOT NULL,\n" +
            "  written_bytes BIGINT NOT NULL,\n" +
            "  written_rows BIGINT NOT NULL,\n" +
            "  cumulative_memory DOUBLE NOT NULL,\n" +
            "  failed_cumulative_memory DOUBLE NOT NULL,\n" +
            "  completed_splits BIGINT NOT NULL,\n" +
            "  retry_policy VARCHAR(255) NOT NULL\n" +
            ")")
    void createTable();

    @SqlUpdate("INSERT INTO trino_queries (\n" +
            "  query_id,\n" +
            "  transaction_id,\n" +
            "  query,\n" +
            "  update_type,\n" +
            "  prepared_query,\n" +
            "  query_state,\n" +
            "  plan,\n" +
            "  stage_info_json,\n" +
            "  user,\n" +
            "  principal,\n" +
            "  trace_token,\n" +
            "  remote_client_address,\n" +
            "  user_agent,\n" +
            "  client_info,\n" +
            "  client_tags_json,\n" +
            "  source,\n" +
            "  catalog,\n" +
            "  `schema`,\n" +
            "  resource_group_id,\n" +
            "  session_properties_json,\n" +
            "  server_address,\n" +
            "  server_version,\n" +
            "  environment,\n" +
            "  query_type,\n" +
            "  inputs_json,\n" +
            "  output_json,\n" +
            "  error_code,\n" +
            "  error_type,\n" +
            "  failure_type,\n" +
            "  failure_message,\n" +
            "  failure_task,\n" +
            "  failure_host,\n" +
            "  failures_json,\n" +
            "  warnings_json,\n" +
            "  cpu_time_millis,\n" +
            "  failed_cpu_time_millis,\n" +
            "  wall_time_millis,\n" +
            "  queued_time_millis,\n" +
            "  scheduled_time_millis,\n" +
            "  failed_scheduled_time_millis,\n" +
            "  waiting_time_millis,\n" +
            "  analysis_time_millis,\n" +
            "  planning_time_millis,\n" +
            "  execution_time_millis,\n" +
            "  input_blocked_time_millis,\n" +
            "  failed_input_blocked_time_millis,\n" +
            "  output_blocked_time_millis,\n" +
            "  failed_output_blocked_time_millis,\n" +
            "  physical_input_read_time_millis,\n" +
            "  peak_memory_bytes,\n" +
            "  peak_task_memory_bytes,\n" +
            "  physical_input_bytes,\n" +
            "  physical_input_rows,\n" +
            "  internal_network_bytes,\n" +
            "  internal_network_rows,\n" +
            "  total_bytes,\n" +
            "  total_rows,\n" +
            "  output_bytes,\n" +
            "  output_rows,\n" +
            "  written_bytes,\n" +
            "  written_rows,\n" +
            "  cumulative_memory,\n" +
            "  failed_cumulative_memory,\n" +
            "  completed_splits,\n" +
            "  retry_policy\n" +
            ")\n" +
            "VALUES (\n" +
            " :queryId,\n" +
            " :transactionId,\n" +
            " :query,\n" +
            " :updateType,\n" +
            " :preparedQuery,\n" +
            " :queryState,\n" +
            " :plan,\n" +
            " :stageInfoJson,\n" +
            " :user,\n" +
            " :principal,\n" +
            " :traceToken,\n" +
            " :remoteClientAddress,\n" +
            " :userAgent,\n" +
            " :clientInfo,\n" +
            " :clientTagsJson,\n" +
            " :source,\n" +
            " :catalog,\n" +
            " :schema,\n" +
            " :resourceGroupId,\n" +
            " :sessionPropertiesJson,\n" +
            " :serverAddress,\n" +
            " :serverVersion,\n" +
            " :environment,\n" +
            " :queryType,\n" +
            " :inputsJson,\n" +
            " :outputJson,\n" +
            " :errorCode,\n" +
            " :errorType,\n" +
            " :failureType,\n" +
            " :failureMessage,\n" +
            " :failureTask,\n" +
            " :failureHost,\n" +
            " :failuresJson,\n" +
            " :warningsJson,\n" +
            " :cpuTimeMillis,\n" +
            " :failedCpuTimeMillis,\n" +
            " :wallTimeMillis,\n" +
            " :queuedTimeMillis,\n" +
            " :scheduledTimeMillis,\n" +
            " :failedScheduledTimeMillis,\n" +
            " :waitingTimeMillis,\n" +
            " :analysisTimeMillis,\n" +
            " :planningTimeMillis,\n" +
            " :executionTimeMillis,\n" +
            " :inputBlockedTimeMillis,\n" +
            " :failedInputBlockedTimeMillis,\n" +
            " :outputBlockedTimeMillis,\n" +
            " :failedOutputBlockedTimeMillis,\n" +
            " :physicalInputReadTimeMillis,\n" +
            " :peakMemoryBytes,\n" +
            " :peakTaskMemoryBytes,\n" +
            " :physicalInputBytes,\n" +
            " :physicalInputRows,\n" +
            " :internalNetworkBytes,\n" +
            " :internalNetworkRows,\n" +
            " :totalBytes,\n" +
            " :totalRows,\n" +
            " :outputBytes,\n" +
            " :outputRows,\n" +
            " :writtenBytes,\n" +
            " :writtenRows,\n" +
            " :cumulativeMemory,\n" +
            " :failedCumulativeMemory,\n" +
            " :completedSplits,\n" +
            " :retryPolicy\n" +
            ")")
    void store(@BindBean QueryEntity entity);
}
