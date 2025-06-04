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
package io.trino.proxy;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.http.server.HttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.JmxModule;
import io.airlift.json.JsonModule;
import io.airlift.log.LogJmxModule;
import io.airlift.log.Logger;
import io.airlift.node.NodeModule;
import io.airlift.tracing.TracingModule;
import org.weakref.jmx.guice.MBeanModule;

import static com.google.common.base.MoreObjects.firstNonNull;

public final class TrinoProxy
{
    private static final String VERSION = firstNonNull(TrinoProxy.class.getPackage().getImplementationVersion(), "unknown");

    private TrinoProxy() {}

    public static void start(Module... extraModules)
    {
        Bootstrap app = new Bootstrap(ImmutableList.<Module>builder()
                .add(new NodeModule())
                .add(new HttpServerModule())
                .add(new JsonModule())
                .add(new JaxrsModule())
                .add(new MBeanModule())
                .add(new JmxModule())
                .add(new LogJmxModule())
                .add(new TracingModule("trino-proxy", VERSION))
                .add(new ProxyModule())
                .add(extraModules)
                .build());

        Logger log = Logger.get(TrinoProxy.class);
        try {
            app.initialize();
            log.info("======== SERVER STARTED ========");
        }
        catch (Throwable t) {
            log.error(t);
            System.exit(1);
        }
    }

    public static void main(String[] args)
    {
        start();
    }
}
