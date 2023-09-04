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
package io.trino.plugin.jdbc;

import io.trino.spi.connector.ConnectorSession;

import static com.google.common.base.Splitter.fixedLength;
import static com.google.common.base.Strings.padStart;

public class ColumnWithAliasFormatter
{
    public static final int DEFAULT_COLUMN_ALIAS_LENGTH = 30;
    public static final int ORIGINAL_COLUMN_NAME_LENGTH = 24;

    public JdbcColumnHandle format(ConnectorSession session, JdbcColumnHandle column, int nextSyntheticColumnId)
    {
        int sequentialNumberLength = DEFAULT_COLUMN_ALIAS_LENGTH - ORIGINAL_COLUMN_NAME_LENGTH - 1;

        String originalColumnNameTruncated = fixedLength(ORIGINAL_COLUMN_NAME_LENGTH)
                .split(column.getColumnName())
                .iterator()
                .next();
        String formatString = "%s_%0" + sequentialNumberLength + "d";
        String columnName = originalColumnNameTruncated + "_" + padStart(Integer.toString(nextSyntheticColumnId), sequentialNumberLength, '0');
        return JdbcColumnHandle.builderFrom(column)
                .setColumnName(columnName)
                .build();
    }
}
