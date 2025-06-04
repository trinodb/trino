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
package io.trino.plugin.hive.util;

public final class SerdeConstants
{
    public static final String SERIALIZATION_LIB = "serialization.lib";
    public static final String SERIALIZATION_NULL_FORMAT = "serialization.null.format";

    public static final String FIELD_DELIM = "field.delim";
    public static final String ESCAPE_CHAR = "escape.delim";

    public static final String HEADER_COUNT = "skip.header.line.count";
    public static final String FOOTER_COUNT = "skip.footer.line.count";

    public static final String LIST_COLUMNS = "columns";
    public static final String LIST_COLUMN_TYPES = "columns.types";
    public static final String LIST_COLUMN_COMMENTS = "columns.comments";

    private SerdeConstants() {}
}
