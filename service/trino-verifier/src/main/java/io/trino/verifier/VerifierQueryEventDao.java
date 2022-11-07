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
    @SqlUpdate("CREATE TABLE IF NOT EXISTS verifier_query_events (\n" +
            "  id BIGINT NOT NULL AUTO_INCREMENT,\n" +
            "  suite VARCHAR(255) NOT NULL,\n" +
            "  run_id VARCHAR(255) NULL,\n" +
            "  source VARCHAR(255) NULL,\n" +
            "  name VARCHAR(255) NULL,\n" +
            "  failed BOOLEAN NOT NULL,\n" +
            "  test_catalog VARCHAR(255) NULL,\n" +
            "  test_schema VARCHAR(255) NULL,\n" +
            "  test_setup_query_ids_json VARCHAR(255) NULL,\n" +
            "  test_query_id VARCHAR(255) NULL,\n" +
            "  test_teardown_query_ids_json VARCHAR(255) NULL,\n" +
            "  test_cpu_time_seconds DOUBLE NULL,\n" +
            "  test_wall_time_seconds DOUBLE NULL,\n" +
            "  control_catalog VARCHAR(255) NULL,\n" +
            "  control_schema VARCHAR(255) NULL,\n" +
            "  control_setup_query_ids_json VARCHAR(255) NULL,\n" +
            "  control_query_id VARCHAR(255) NULL,\n" +
            "  control_teardown_query_ids_json VARCHAR(255) NULL,\n" +
            "  control_cpu_time_seconds DOUBLE NULL,\n" +
            "  control_wall_time_seconds DOUBLE NULL,\n" +
            "  error_message MEDIUMTEXT NULL,\n" +
            "  PRIMARY KEY (id),\n" +
            "  INDEX run_id_name_index(run_id, name)\n" +
            ")")
    void createTable();

    @SqlUpdate("INSERT INTO verifier_query_events (\n" +
            "  suite,\n" +
            "  run_id,\n" +
            "  source,\n" +
            "  name,\n" +
            "  failed,\n" +
            "  test_catalog,\n" +
            "  test_schema,\n" +
            "  test_setup_query_ids_json,\n" +
            "  test_query_id,\n" +
            "  test_teardown_query_ids_json,\n" +
            "  test_cpu_time_seconds,\n" +
            "  test_wall_time_seconds,\n" +
            "  control_catalog,\n" +
            "  control_schema,\n" +
            "  control_setup_query_ids_json,\n" +
            "  control_query_id,\n" +
            "  control_teardown_query_ids_json,\n" +
            "  control_cpu_time_seconds,\n" +
            "  control_wall_time_seconds,\n" +
            "  error_message\n" +
            ")\n" +
            "VALUES (\n" +
            "  :suite,\n" +
            "  :runId,\n" +
            "  :source,\n" +
            "  :name,\n" +
            "  :failed,\n" +
            "  :testCatalog,\n" +
            "  :testSchema,\n" +
            "  :testSetupQueryIdsJson,\n" +
            "  :testQueryId,\n" +
            "  :testTeardownQueryIdsJson,\n" +
            "  :testCpuTimeSeconds,\n" +
            "  :testWallTimeSeconds,\n" +
            "  :controlCatalog,\n" +
            "  :controlSchema,\n" +
            "  :controlSetupQueryIdsJson,\n" +
            "  :controlQueryId,\n" +
            "  :controlTeardownQueryIdsJson,\n" +
            "  :controlCpuTimeSeconds,\n" +
            "  :controlWallTimeSeconds,\n" +
            "  :errorMessage\n" +
            ")")
    void store(@BindBean VerifierQueryEventEntity entity);
}
