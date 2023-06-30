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
package io.trino.plugin.hive.avro;

public final class AvroHiveConstants
{
    private AvroHiveConstants() {}

    // file metadata
    public static final String WRITER_TIME_ZONE = "writer.time.zone";

    //hive table properties
    public static final String SCHEMA_LITERAL = "avro.schema.literal";
    public static final String SCHEMA_URL = "avro.schema.url";
    public static final String SCHEMA_NONE = "none";
    public static final String SCHEMA_NAMESPACE = "avro.schema.namespace";
    public static final String SCHEMA_NAME = "avro.schema.name";
    public static final String SCHEMA_DOC = "avro.schema.doc";
    public static final String TABLE_NAME = "name";

    // Hive Logical types
    public static final String CHAR_TYPE_LOGICAL_NAME = "char";
    public static final String VARCHAR_TYPE_LOGICAL_NAME = "varchar";
    public static final String VARCHAR_AND_CHAR_LOGICAL_TYPE_LENGTH_PROP = "maxLength";
}
