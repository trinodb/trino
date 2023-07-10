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
package io.trino.plugin.exchange.hdfs;

import com.google.inject.Inject;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class ExchangeHdfsEnvironment
{
    private final HdfsEnvironment hdfsEnvironment;
    private final ExchangeHdfsConfig exchangeHdfsConfig;

    @Inject
    public ExchangeHdfsEnvironment(ExchangeHdfsConfig exchangeHdfsConfig, HdfsEnvironment hdfsEnvironment)
    {
        this.exchangeHdfsConfig = requireNonNull(exchangeHdfsConfig, "exchangeHdfsConfig is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    public FileSystem getFileSystem(Path path)
            throws IOException
    {
        ClassLoader classLoader = ExchangeHdfsEnvironment.class.getClassLoader();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            HdfsContext hdfsContext = new HdfsContext(ConnectorIdentity.forUser(exchangeHdfsConfig.getHdfsProxyUser().orElse("")).build());
            return hdfsEnvironment.getFileSystem(hdfsContext, path);
        }
    }
}
