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
package io.trino.plugin.varada.storage.engine.nativeimpl;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;

@Singleton
public final class NativeLogger
{
    private static final Logger logger = Logger.get(NativeLogger.class);

    @Inject
    public NativeLogger()
    {
        load();
    }

    // @TODO add more levels
    public static void logger_debug(String msg)
    {
        logger.debug(msg);
    }

    public static void logger_normal(String msg)
    {
        logger.info(msg);
    }

    public static void logger_critical(String msg)
    {
        logger.error(msg);
    }

    private static native void nativeInit();

    private void load()
    {
        nativeInit();
    }
}
