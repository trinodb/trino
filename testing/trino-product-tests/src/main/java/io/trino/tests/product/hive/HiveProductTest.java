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
package io.trino.tests.product.hive;

import com.google.inject.Inject;
import io.trino.tempto.ProductTest;

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

    protected boolean isHiveWithBrokenAvroTimestamps()
    {
        // In 3.1.0 timestamp semantics in hive changed in backward incompatible way,
        // which was fixed for Parquet and Avro in 3.1.2 (https://issues.apache.org/jira/browse/HIVE-21002)
        // we do have a work-around for Parquet, but still need this for Avro until
        // https://github.com/trinodb/trino/issues/5144 is addressed
        return getHiveVersionMajor() == 3 &&
                getHiveVersionMinor() == 1 &&
                (getHiveVersionPatch() == 0 || getHiveVersionPatch() == 1);
    }
}
