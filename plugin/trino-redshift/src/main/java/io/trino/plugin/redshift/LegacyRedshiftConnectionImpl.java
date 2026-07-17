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
import io.trino.plugin.jdbc.ForwardingConnection;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

/**
 * A connection with small metadata behavior patches.
 */
public class LegacyRedshiftConnectionImpl
        extends ForwardingConnection
{
    private final RedshiftConnectionImpl delegate;
    private DatabaseMetaData metadata;

    public LegacyRedshiftConnectionImpl(RedshiftConnectionImpl delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public DatabaseMetaData getMetaData()
            throws SQLException
    {
        // https://github.com/aws/amazon-redshift-jdbc-driver/blob/de46a92726824ef1c1d0f52eb4aa4da0752538b5/src/main/java/com/amazon/redshift/jdbc/RedshiftConnectionImpl.java#L1582
        if (isClosed()) {
            throw new SQLException("Connection is closed");
        }
        if (this.metadata == null) {
            this.metadata = new LegacyRedshiftDatabaseMetaData(delegate);
        }
        return this.metadata;
    }

    @Override
    protected Connection delegate()
    {
        return delegate;
    }
}
