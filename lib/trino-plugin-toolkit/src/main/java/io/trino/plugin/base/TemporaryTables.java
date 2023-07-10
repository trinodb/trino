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
package io.trino.plugin.base;

import io.trino.spi.connector.ConnectorSession;

import java.util.HexFormat;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.Objects.requireNonNull;

public class TemporaryTables
{
    private static final HexFormat hexFormat = HexFormat.of();
    public static final String TEMPORARY_TABLE_NAME_PREFIX = "tmp_trino_";

    public static String temporaryTableNamePrefix(String queryId)
    {
        requireNonNull(queryId, "queryId is null");
        return String.format("%s%s_", TEMPORARY_TABLE_NAME_PREFIX, hexFormat.toHexDigits(queryId.hashCode()));
    }

    public static String generateTemporaryTableName(String queryId)
    {
        requireNonNull(queryId, "queryId is null");
        return temporaryTableNamePrefix(queryId) + hexFormat.toHexDigits(ThreadLocalRandom.current().nextInt());
    }

    public static String generateTemporaryTableName(ConnectorSession session)
    {
        requireNonNull(session, "session is null");
        return generateTemporaryTableName(session.getQueryId());
    }

    private TemporaryTables()
    {
    }
}
