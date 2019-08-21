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
package io.prestosql.plugin.hive;

import org.apache.hadoop.net.NetUtils;
import org.testng.SkipException;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;

import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

public class TestHive
        extends AbstractTestHive
{
    private String hiveVersion;

    @Parameters({"hive.hadoop2.metastoreHost", "hive.hadoop2.metastorePort", "hive.hadoop2.databaseName", "hive.hadoop2.timeZone", "hive.hadoop2.hiveVersion"})
    @BeforeClass
    public void initialize(String host, int port, String databaseName, String timeZone, String hiveVersion)
            throws UnknownHostException
    {
        String hadoopMasterIp = System.getProperty("hadoop-master-ip");
        if (hadoopMasterIp != null) {
            // Even though Hadoop is accessed by proxy, Hadoop still tries to resolve hadoop-master
            // (e.g: in: NameNodeProxies.createProxy)
            // This adds a static resolution for hadoop-master to docker container internal ip
            NetUtils.addStaticResolution("hadoop-master", hadoopMasterIp);
        }

        setup(host, port, databaseName, timeZone);

        this.hiveVersion = requireNonNull(hiveVersion, "hiveVersion is null");
    }

    private String getHiveVersion()
    {
        checkState(hiveVersion != null, "hiveVersion not set");
        return hiveVersion;
    }

    private int getHiveVersionMajor()
    {
        String hiveVersion = getHiveVersion();
        Matcher matcher = Pattern.compile("^(\\d+)\\.").matcher(hiveVersion);
        checkState(matcher.lookingAt(), "Invalid Hive version: %s", hiveVersion);
        return parseInt(matcher.group(1));
    }

    @Override
    public void testGetPartitionSplitsTableOfflinePartition()
    {
        if (getHiveVersionMajor() >= 2) {
            throw new SkipException("ALTER TABLE .. ENABLE OFFLINE was removed in Hive 2.0 and this is a prerequisite for this test");
        }

        super.testGetPartitionSplitsTableOfflinePartition();
    }
}
