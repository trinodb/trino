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

import com.amazon.redshift.jdbc.Driver;
import com.amazon.redshift.jdbc.RedshiftConnectionImpl;
import io.trino.spi.TrinoException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

/**
 * Driver that returns a connection with patched connection behavior.
 */
public class LegacyRedshiftDriver
        extends Driver
{
    @Override
    public Connection connect(String url, Properties info)
            throws SQLException
    {
        @SuppressWarnings("resource") // Wrapper class will close underlying connection.
        Connection wrapped = super.connect(url, info);
        if (wrapped == null) {
            return null;
        }
        if (wrapped instanceof RedshiftConnectionImpl redshiftConnectionImpl) {
            // We check type directly instead of using unwrap,
            // since implementation details matters to the patch.
            return new LegacyRedshiftConnectionImpl(redshiftConnectionImpl);
        }
        throw new TrinoException(
                GENERIC_INTERNAL_ERROR,
                "Unexpected redshift connection class %s".formatted(wrapped.getClass().getCanonicalName()));
    }
}
