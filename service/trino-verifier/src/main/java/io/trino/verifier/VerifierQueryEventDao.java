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
package io.trino.verifier;

import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public interface VerifierQueryEventDao
{
    @SqlUpdate(
            """
            CREATE TABLE IF NOT EXISTS verifier_query_events (
              id BIGINT NOT NULL AUTO_INCREMENT,
              suite VARCHAR(255) NOT NULL,
              run_id VARCHAR(255) NULL,
              source VARCHAR(255) NULL,
              name VARCHAR(255) NULL,
              failed BOOLEAN NOT NULL,
              test_catalog VARCHAR(255) NULL,
              test_schema VARCHAR(255) NULL,
              test_setup_query_ids_json VARCHAR(255) NULL,
              test_query_id VARCHAR(255) NULL,
              test_teardown_query_ids_json VARCHAR(255) NULL,
              test_cpu_time_seconds DOUBLE NULL,
              test_wall_time_seconds DOUBLE NULL,
              control_catalog VARCHAR(255) NULL,
              control_schema VARCHAR(255) NULL,
              control_setup_query_ids_json VARCHAR(255) NULL,
              control_query_id VARCHAR(255) NULL,
              control_teardown_query_ids_json VARCHAR(255) NULL,
              control_cpu_time_seconds DOUBLE NULL,
              control_wall_time_seconds DOUBLE NULL,
              error_message MEDIUMTEXT NULL,
              PRIMARY KEY (id),
              INDEX run_id_name_index(run_id, name)
            )
            """)
    void createTable();

    @SqlUpdate(
            """
            INSERT INTO verifier_query_events (
              suite,
              run_id,
              source,
              name,
              failed,
              test_catalog,
              test_schema,
              test_setup_query_ids_json,
              test_query_id,
              test_teardown_query_ids_json,
              test_cpu_time_seconds,
              test_wall_time_seconds,
              control_catalog,
              control_schema,
              control_setup_query_ids_json,
              control_query_id,
              control_teardown_query_ids_json,
              control_cpu_time_seconds,
              control_wall_time_seconds,
              error_message
            )
            VALUES (
              :suite,
              :runId,
              :source,
              :name,
              :failed,
              :testCatalog,
              :testSchema,
              :testSetupQueryIdsJson,
              :testQueryId,
              :testTeardownQueryIdsJson,
              :testCpuTimeSeconds,
              :testWallTimeSeconds,
              :controlCatalog,
              :controlSchema,
              :controlSetupQueryIdsJson,
              :controlQueryId,
              :controlTeardownQueryIdsJson,
              :controlCpuTimeSeconds,
              :controlWallTimeSeconds,
              :errorMessage
            )
            """)
    void store(@BindBean VerifierQueryEventEntity entity);
}
