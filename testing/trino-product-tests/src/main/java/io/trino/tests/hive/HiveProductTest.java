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
package io.trino.tests.hive;

import io.trino.tempto.ProductTest;
import org.intellij.lang.annotations.Language;

import javax.inject.Inject;

public class HiveProductTest
        extends ProductTest
{
    static final String ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE = "https://github.com/trinodb/trino/issues/4936";
    @Language("RegExp")
    static final String ERROR_COMMITTING_WRITE_TO_HIVE_MATCH = "Error committing write to Hive(?s:.*)" +
            // "could only be written to 0 of the 1 minReplication" is the error wording used by e.g. HDP 3
            "(could only be replicated to 0 nodes instead of minReplication|could only be written to 0 of the 1 minReplication)";

    @Inject
    private HiveVersionProvider hiveVersionProvider;

    protected int getHiveVersionMajor()
    {
        return hiveVersionProvider.getHiveVersion().getMajorVersion();
    }

    protected int getHiveVersionMinor()
    {
        return hiveVersionProvider.getHiveVersion().getMinorVersion();
    }

    protected int getHiveVersionPatch()
    {
        return hiveVersionProvider.getHiveVersion().getPatchVersion();
    }

    protected boolean isHiveVersionBefore12()
    {
        return getHiveVersionMajor() == 0
                || (getHiveVersionMajor() == 1 && getHiveVersionMinor() < 2);
    }
}
