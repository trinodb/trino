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
package io.trino.metastore.type;

public final class TypeConstants
{
    private TypeConstants() {}

    public static final String VOID_TYPE_NAME = "void";
    public static final String BOOLEAN_TYPE_NAME = "boolean";
    public static final String TINYINT_TYPE_NAME = "tinyint";
    public static final String SMALLINT_TYPE_NAME = "smallint";
    public static final String INT_TYPE_NAME = "int";
    public static final String BIGINT_TYPE_NAME = "bigint";
    public static final String FLOAT_TYPE_NAME = "float";
    public static final String DOUBLE_TYPE_NAME = "double";
    public static final String STRING_TYPE_NAME = "string";
    public static final String CHAR_TYPE_NAME = "char";
    public static final String VARCHAR_TYPE_NAME = "varchar";
    public static final String DATE_TYPE_NAME = "date";
    public static final String TIMESTAMP_TYPE_NAME = "timestamp";
    public static final String TIMESTAMPLOCALTZ_TYPE_NAME = "timestamp with local time zone";
    public static final String DECIMAL_TYPE_NAME = "decimal";
    public static final String BINARY_TYPE_NAME = "binary";
    public static final String INTERVAL_YEAR_MONTH_TYPE_NAME = "interval_year_month";
    public static final String INTERVAL_DAY_TIME_TYPE_NAME = "interval_day_time";
    public static final String VARIANT_TYPE_NAME = "variant";

    public static final String LIST_TYPE_NAME = "array";
    public static final String MAP_TYPE_NAME = "map";
    public static final String STRUCT_TYPE_NAME = "struct";
    public static final String UNION_TYPE_NAME = "uniontype";
}
