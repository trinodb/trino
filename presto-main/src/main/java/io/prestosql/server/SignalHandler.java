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
package io.prestosql.server;

import io.airlift.log.Logger;
import sun.misc.Signal;

import static java.util.Objects.requireNonNull;

public class SignalHandler
            implements sun.misc.SignalHandler
{
    private static final Logger log = Logger.get(GracefulShutdownHandler.class);
    private final GracefulShutdownHandler shutdownHandler;

    public SignalHandler(GracefulShutdownHandler shutdownHandler)
    {
        this.shutdownHandler = requireNonNull(shutdownHandler, "shutdownHandler is null");
    }

    public static void listenTo(ServerConfig config, GracefulShutdownHandler shutdownHandler)
    {
        if (config.getGracefulShutdownSignal() != null) {
            Signal signal = new Signal(config.getGracefulShutdownSignal());
            Signal.handle(signal, new SignalHandler(shutdownHandler));
        }
    }

    public void handle(Signal signal)
    {
        log.info(signal.toString().trim() + " raised, requesting graceful shutdown...");
        shutdownHandler.requestShutdown();
    }
}
