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
package io.trino.plugin.iceberg;

import io.trino.Session;
import org.testng.annotations.Test;

import static io.trino.plugin.iceberg.IcebergFileFormat.ORC;
import static io.trino.plugin.iceberg.IcebergTestUtil.orcSupportsIcebergFileStatistics;
import static io.trino.plugin.iceberg.IcebergTestUtil.orcSupportsRowGroupStatistics;
import static io.trino.plugin.iceberg.IcebergTestUtil.orcWithSmallRowGroups;

// Due to fact some tests running on MinIO are memory consuming, we want to prevent running them in parallel.
@Test(singleThreaded = true)
public class TestIcebergMinioOrcConnectorTest
        extends BaseIcebergMinioConnectorTest
{
    public TestIcebergMinioOrcConnectorTest()
    {
        super(ORC);
    }

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        return orcSupportsIcebergFileStatistics(typeName);
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        return orcSupportsRowGroupStatistics(typeName);
    }

    @Override
    protected Session withSmallRowGroups(Session session)
    {
        return orcWithSmallRowGroups(session);
    }
}
