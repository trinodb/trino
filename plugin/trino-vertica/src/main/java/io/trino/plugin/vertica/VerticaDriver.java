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
package io.trino.plugin.vertica;

import com.vertica.core.VConnectionPropertyKey;
import com.vertica.core.VDriver;
import com.vertica.jdbc.hybrid.HybridAbstractDriver;
import com.vertica.utilities.JDBCVersion;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Original Driver class has broken JDK version detection that throws during initialization.
 * This version works correctly on JDK 17 and beyond.
 *
 * See: com.vertica.jdbc.Driver.runningJDBCVersion
 */
public class VerticaDriver
        extends HybridAbstractDriver
{
    @Override
    protected JDBCVersion runningJDBCVersion()
    {
        return JDBCVersion.JDBC42;
    }

    @Override
    protected String getSubProtocol()
    {
        return "vertica";
    }

    @Override
    protected boolean parseSubName(String subName, Properties properties)
    {
        return VConnectionPropertyKey.parseSubName(subName, properties);
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        throw new SQLFeatureNotSupportedException("java.util.logging not used");
    }

    static {
        try {
            HybridAbstractDriver.initialize(new VerticaDriver(), VDriver.class.getName());
            HybridAbstractDriver.setErrorMessageComponentName("Vertica");
        }
        catch (SQLException _) {
        }
    }
}
