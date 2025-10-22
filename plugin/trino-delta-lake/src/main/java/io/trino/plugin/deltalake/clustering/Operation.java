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
package io.trino.plugin.deltalake.clustering;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Locale.ENGLISH;

public enum Operation
{
    // refer to: https://github.com/delta-io/delta/blob/master/spark/src/main/scala/org/apache/spark/sql/delta/DeltaOperations.scala
    ADD_COLUMNS("ADD COLUMNS"),
    ADD_CONSTRAINT("ADD CONSTRAINT"),
    ADD_DELETION_VECTOR_TOMBSTONES("Deletion Vector Tombstones"),
    CHANGE_COLUMN("CHANGE COLUMN"),
    CHANGE_COLUMNS("CHANGE COLUMNS"),
    CLONE("CLONE"),
    CLUSTER_BY("CLUSTER BY"),
    COMPUTE_STATS("COMPUTE STATS"),
    CONVERT("CONVERT"),
    DELETE("DELETE"),
    DOMAIN_METADATA_CLEANUP("DOMAIN METADATA CLEANUP"),
    DROP_COLUMNS("DROP COLUMNS"),
    DROP_CONSTRAINT("DROP CONSTRAINT"),
    DROP_TABLE_FEATURE("DROP FEATURE"),
    EMPTY_COMMIT("Empty Commit"),
    MANUAL_UPDATE("Manual Update"),
    MERGE("MERGE"),
    OPTIMIZE("OPTIMIZE"),
    RENAME_COLUMN("RENAME COLUMN"),
    REORG("REORG"),
    REORG_TABLE_UPGRADE_UNIFORM("REORG TABLE UPGRADE UNIFORM"),
    REMOVE_COLUMN_MAPPING("REMOVE COLUMN MAPPING"),
    REPLACE_COLUMNS("REPLACE COLUMNS"),
    RESTORE("RESTORE"),
    ROW_TRACKING_BACKFILL("ROW TRACKING BACKFILL"),
    ROW_TRACKING_UNBACKFILL("ROW TRACKING UNBACKFILL"),
    SET_TABLE_PROPERTIES("SET TBLPROPERTIES"),
    STREAMING_UPDATE("STREAMING UPDATE"),
    TRUNCATE("TRUNCATE"),
    UNSET_TABLE_PROPERTIES("UNSET TBLPROPERTIES"),
    UPDATE("UPDATE"),
    UPDATE_COLUMN_METADATA("UPDATE COLUMN METADATA"),
    UPDATE_SCHEMA("UPDATE SCHEMA"),
    UPGRADE_PROTOCOL("UPGRADE PROTOCOL"),
    VACUUM_END("VACUUM END"),
    VACUUM_START("VACUUM START"),
    WRITE("WRITE"),

    TEST_OPERATION("TEST"),

    CREATE_TABLE_KEYWORD("CREATE TABLE"),
    REPLACE_TABLE_KEYWORD("REPLACE TABLE"),

    UNKNOW_OPERATION("UNKNOWN OPERATION");

    private final String operationName;

    Operation(String operationName)
    {
        this.operationName = operationName;
    }

    public String getOperationName()
    {
        return operationName;
    }

    @Override
    public String toString()
    {
        return operationName;
    }

    private static final Map<String, Operation> LOWERCASE_NAME_TO_OPERATION = Stream.of(values())
            .collect(toImmutableMap(op -> op.operationName.toLowerCase(ENGLISH), op -> op));

    private static final Map<String, Operation> LOWERCASE_KEYWORD_TO_OPERATION = ImmutableMap.of(
            CREATE_TABLE_KEYWORD.getOperationName().toLowerCase(ENGLISH), CREATE_TABLE_KEYWORD,
            REPLACE_TABLE_KEYWORD.getOperationName().toLowerCase(ENGLISH), REPLACE_TABLE_KEYWORD);

    public static Operation fromString(String operationName)
    {
        Operation operation = LOWERCASE_NAME_TO_OPERATION.get(operationName.toLowerCase(ENGLISH));
        if (operation == null) {
            for (String keyword : LOWERCASE_KEYWORD_TO_OPERATION.keySet()) {
                if (operationName.toLowerCase(ENGLISH).contains(keyword)) {
                    operation = LOWERCASE_NAME_TO_OPERATION.get(keyword);
                    break;
                }
            }
        }
        return operation == null ? UNKNOW_OPERATION : operation;
    }
}
