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
package io.prestosql.tests.hive;

import io.prestosql.tempto.ProductTest;

import javax.inject.Inject;

public class HiveProductTest
        extends ProductTest
{
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
