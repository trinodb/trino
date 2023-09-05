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

import static com.google.common.base.Splitter.fixedLength;
import static com.google.common.base.Verify.verify;

public class SyntheticColumnHandleBuilder
{
    public static final int DEFAULT_COLUMN_ALIAS_LENGTH = 30;

    public JdbcColumnHandle get(JdbcColumnHandle column, int nextSyntheticColumnId)
    {
        verify(nextSyntheticColumnId >= 0, "nextSyntheticColumnId rolled over and is not monotonically increasing any more");

        int sequentialNumberLength = String.valueOf(nextSyntheticColumnId).length();
        int originalColumnNameLength = DEFAULT_COLUMN_ALIAS_LENGTH - sequentialNumberLength - "_".length();

        String columnNameTruncated = fixedLength(originalColumnNameLength)
                .split(column.getColumnName())
                .iterator()
                .next();
        String columnName = columnNameTruncated + "_" + nextSyntheticColumnId;
        return JdbcColumnHandle.builderFrom(column)
                .setColumnName(columnName)
                .build();
    }
}
