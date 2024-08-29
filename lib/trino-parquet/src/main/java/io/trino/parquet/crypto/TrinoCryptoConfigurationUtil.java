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
package io.trino.parquet.crypto;

import io.airlift.log.Logger;

public class TrinoCryptoConfigurationUtil
{
    public static final Logger LOG = Logger.get(TrinoCryptoConfigurationUtil.class);

    private TrinoCryptoConfigurationUtil()
    {
    }

    public static Class<?> getClassFromConfig(String className, Class<?> assignableFrom)
    {
        try {
            final Class<?> foundClass = Class.forName(className);
            if (!assignableFrom.isAssignableFrom(foundClass)) {
                LOG.warn("class " + className + " is not a subclass of " + assignableFrom.getCanonicalName());
                return null;
            }
            return foundClass;
        }
        catch (ClassNotFoundException e) {
            LOG.warn("could not instantiate class " + className, e);
            return null;
        }
    }
}
