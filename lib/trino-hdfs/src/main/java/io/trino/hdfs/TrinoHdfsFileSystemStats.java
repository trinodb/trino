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
package io.trino.hdfs;

import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public final class TrinoHdfsFileSystemStats
{
    private final CallStats openFileCalls = new CallStats();
    private final CallStats createFileCalls = new CallStats();
    private final CallStats listFilesCalls = new CallStats();
    private final CallStats renameFileCalls = new CallStats();
    private final CallStats deleteFileCalls = new CallStats();
    private final CallStats deleteDirectoryCalls = new CallStats();
    private final CallStats directoryExistsCalls = new CallStats();
    private final CallStats createDirectoryCalls = new CallStats();
    private final CallStats renameDirectoryCalls = new CallStats();

    @Managed
    @Nested
    public CallStats getOpenFileCalls()
    {
        return openFileCalls;
    }

    @Managed
    @Nested
    public CallStats getCreateFileCalls()
    {
        return createFileCalls;
    }

    @Managed
    @Nested
    public CallStats getListFilesCalls()
    {
        return listFilesCalls;
    }

    @Managed
    @Nested
    public CallStats getRenameFileCalls()
    {
        return renameFileCalls;
    }

    @Managed
    @Nested
    public CallStats getDeleteFileCalls()
    {
        return deleteFileCalls;
    }

    @Managed
    @Nested
    public CallStats getDeleteDirectoryCalls()
    {
        return deleteDirectoryCalls;
    }

    @Managed
    @Nested
    public CallStats getDirectoryExistsCalls()
    {
        return directoryExistsCalls;
    }

    @Managed
    @Nested
    public CallStats getCreateDirectoryCalls()
    {
        return createDirectoryCalls;
    }

    @Managed
    @Nested
    public CallStats getRenameDirectoryCalls()
    {
        return renameDirectoryCalls;
    }
}
