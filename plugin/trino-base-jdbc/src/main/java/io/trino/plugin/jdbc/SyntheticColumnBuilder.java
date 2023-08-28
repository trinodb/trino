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

import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;

public class SyntheticColumnBuilder
{
    private static final String SEPARATOR = "_";
    private static final int SEPARATOR_LENGTH = SEPARATOR.length();

    public JdbcColumnHandle aliasForColumn(JdbcColumnHandle column, OptionalInt maximumIdentifierLength, int nextSyntheticColumnId)
    {
        if (maximumIdentifierLength.isEmpty()) {
            // No identifier length limit
            return syntheticColumnHandle(column, nextSyntheticColumnId);
        }

        String nextColumnId = String.valueOf(nextSyntheticColumnId);
        int nextColumnIdLength = nextColumnId.length();

        int maximumLength = maximumIdentifierLength.getAsInt();
        int columnLength = column.getColumnName().length();

        int totalLength = columnLength + nextColumnIdLength + SEPARATOR_LENGTH;
        verify(nextColumnIdLength <= maximumLength, "Maximum allowed identifier length is %s but synthetic column has length %s", maximumLength, nextColumnIdLength);

        if (nextColumnIdLength == maximumLength) {
            return JdbcColumnHandle.builderFrom(column)
                    .setColumnName(nextColumnId)
                    .build();
        }

        if (totalLength > maximumLength) {
            String strippedColumnName = column.getColumnName().substring(0, columnLength - (totalLength - maximumLength));
            return JdbcColumnHandle.builderFrom(column)
                    .setColumnName(strippedColumnName + SEPARATOR + nextColumnId)
                    .build();
        }

        return syntheticColumnHandle(column, nextSyntheticColumnId);
    }

    private static JdbcColumnHandle syntheticColumnHandle(JdbcColumnHandle column, int nextSyntheticColumnId)
    {
        return JdbcColumnHandle.builderFrom(column)
                .setColumnName(column.getColumnName() + SEPARATOR + nextSyntheticColumnId)
                .build();
    }
}
