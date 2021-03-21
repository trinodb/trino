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
package io.trino.spi.connector;

public enum RowChangeParadigm
{
    /**
     * The JDBC paradigm - - requires just the rowId and the changed columns
     */
    CHANGE_ONLY_UPDATED_COLUMNS,

    /**
     * The Hive and Iceberg paradigm - - translates a changed row into a delete
     * by rowId, and an insert of a new record, which will get a new rowId
     */
    DELETE_ROW_AND_INSERT_ROW,

    /**
     * The Delta Lake paradigm - - to change any field of any row, the entire file
     * must be replaced
     */
    COPY_FILE_ON_CHANGE,
}
