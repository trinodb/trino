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

import com.amazon.redshift.util.RedshiftException;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import jakarta.validation.constraints.AssertTrue;

import java.util.Enumeration;

import static com.amazon.redshift.Driver.parseURL;

public class RedshiftJdbcConfig
        extends BaseJdbcConfig
{
    @AssertTrue(message = "JDBC URL for redshift connector may contain illegal datatype parameters (CVE-2026-8178)")
    public boolean isUrlValid()
            throws RedshiftException
    {
        Enumeration<?> propertyNames = parseURL(getConnectionUrl(), null).propertyNames();
        while (propertyNames.hasMoreElements()) {
            // Looking for things that use this recently removed classloading for CVE-2026-8178.
            // https://github.com/aws/amazon-redshift-jdbc-driver/blob/67cc11b3c7aeaa417bd7c830ccfb2841e6977820/src/main/java/com/amazon/redshift/jdbc/RedshiftConnectionImpl.java#L903
            if (((String) propertyNames.nextElement()).startsWith("datatype.")) {
                return false;
            }
        }
        return true;
    }
}
