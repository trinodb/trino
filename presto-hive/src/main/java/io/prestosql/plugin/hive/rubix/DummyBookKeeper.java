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
package io.prestosql.plugin.hive.rubix;

import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.BookKeeperService;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.FileInfo;
import com.qubole.rubix.spi.thrift.HeartbeatStatus;

import java.util.List;
import java.util.Map;

public class DummyBookKeeper
        implements BookKeeperService.Iface
{
    @Override
    public List<BlockLocation> getCacheStatus(CacheStatusRequest cacheStatusRequest)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setAllCached(String remotePath, long fileLength, long lastModified, long startBlock, long endBlock)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Double> getCacheMetrics()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean readData(String remotePath, long offset, int length, long fileSize, long lastModified, int clusterType)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleHeartbeat(String workerHostname, HeartbeatStatus heartbeatStatus)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileInfo getFileInfo(String remotePath)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBookKeeperAlive()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void invalidateFileMetadata(String remotePath)
    {
        throw new UnsupportedOperationException();
    }
}
