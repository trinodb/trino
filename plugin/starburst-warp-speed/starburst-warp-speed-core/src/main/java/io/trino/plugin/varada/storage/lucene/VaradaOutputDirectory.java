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
package io.trino.plugin.varada.storage.lucene;

import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NoLockFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

public class VaradaOutputDirectory
        extends BaseDirectory
{
    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final WriteJuffersWarmUpElement juffersWE;
    private final long weCookie;
    private final ByteBuffersDirectory byteBuffersDirectory;

    protected VaradaOutputDirectory(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            WriteJuffersWarmUpElement juffersWE,
            long weCookie)
    {
        super(NoLockFactory.INSTANCE);
        this.byteBuffersDirectory = new ByteBuffersDirectory(NoLockFactory.INSTANCE);
        this.storageEngine = storageEngine;
        this.storageEngineConstants = storageEngineConstants;
        this.juffersWE = juffersWE;
        this.weCookie = weCookie;
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context)
    {
        return new VaradaIndexOutput(name, juffersWE, storageEngineConstants, storageEngine, weCookie, true);
    }

    @Override
    public String[] listAll()
            throws IOException
    {
        return byteBuffersDirectory.listAll();
    }

    @Override
    public void deleteFile(String name)
            throws IOException
    {
        byteBuffersDirectory.deleteFile(name);
    }

    @Override
    public long fileLength(String name)
            throws IOException
    {
        return byteBuffersDirectory.fileLength(name);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
            throws IOException
    {
        return byteBuffersDirectory.createTempOutput(prefix, suffix, context);
    }

    @Override
    public void rename(String source, String dest)
            throws IOException
    {
        byteBuffersDirectory.rename(source, dest);
    }

    @Override
    public void sync(Collection<String> names)
            throws IOException
    {
        byteBuffersDirectory.sync(names);
    }

    @Override
    public void syncMetaData()
            throws IOException
    {
        byteBuffersDirectory.syncMetaData();
    }

    @Override
    public IndexInput openInput(String name, IOContext context)
            throws IOException
    {
        return byteBuffersDirectory.openInput(name, context);
    }

    @Override
    public void close()
            throws IOException
    {
        byteBuffersDirectory.close();
    }

    @Override
    public Set<String> getPendingDeletions()
    {
        return byteBuffersDirectory.getPendingDeletions();
    }
}
