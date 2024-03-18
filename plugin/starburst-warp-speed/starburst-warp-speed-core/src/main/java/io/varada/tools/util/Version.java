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
package io.varada.tools.util;

import io.airlift.log.Logger;

@SuppressWarnings("rawtypes")
public final class Version
{
    public static final String DEV_VERSION = "DEV";
    private static final Logger logger = Logger.get(Version.class);
    private static final Version INSTANCE = new Version();
    private final String version;

    private Version()
    {
        version = loadVersion(Version.class, true);
    }

    public static Version getInstance()
    {
        return INSTANCE;
    }

    private static String loadVersion(Class clazz, boolean impl)
    {
        Package objPackage = clazz.getPackage();
        //some jars may use 'Implementation Version' entries in the manifest instead
        String version = objPackage.getSpecificationVersion();
        if (impl && StringUtils.isNotEmpty(objPackage.getImplementationVersion())) {
            version = objPackage.getImplementationVersion();
        }

        if (StringUtils.isNotEmpty(version)) {
            return version;
        }
        logger.warn("Setting version to %s because we're not running from a jar", DEV_VERSION);
        return DEV_VERSION;
    }

    public String getVersion()
    {
        return version;
    }
}
