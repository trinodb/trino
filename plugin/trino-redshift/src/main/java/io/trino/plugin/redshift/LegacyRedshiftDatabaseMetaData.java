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
package io.trino.plugin.redshift;

import com.amazon.redshift.jdbc.RedshiftConnectionImpl;
import com.amazon.redshift.jdbc.RedshiftDatabaseMetaData;

import java.sql.SQLException;

/**
 * A RedshiftMetaData wrapper that turns off "SHOW" discovery.
 * <p>
 * Enables working around this <a href="https://github.com/aws/amazon-redshift-jdbc-driver/issues/148">bug</a>.
 */
public class LegacyRedshiftDatabaseMetaData
        extends RedshiftDatabaseMetaData
{
    public LegacyRedshiftDatabaseMetaData(RedshiftConnectionImpl conn)
            throws SQLException
    {
        super(conn);
    }

    @Override
    public int supportSHOWDiscovery()
    {
        // Return 0 to use `getTablesLegacyHardcodedQuery` which doesn't show bugs.
        // https://github.com/aws/amazon-redshift-jdbc-driver/blob/de46a92726824ef1c1d0f52eb4aa4da0752538b5/src/main/java/com/amazon/redshift/jdbc/RedshiftDatabaseMetaData.java#L1733
        return 0;
    }
}
