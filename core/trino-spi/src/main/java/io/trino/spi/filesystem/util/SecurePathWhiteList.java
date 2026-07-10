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
package io.trino.spi.filesystem.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SecurePathWhiteList
{
    private static Set<String> securePathwhiteList = new HashSet<>(Arrays.asList(
            "/tmp",
            "/opt/trino",
            "/etc/trino",
            "/etc/security",
            System.getProperty("java.io.tmpdir")));

    private SecurePathWhiteList()
    {
    }

    /**
     * Due to security concerns, all data must be read from one of the whitelisted paths
     *
     * @return secure path white list
     * @throws IOException if workspace is root directory
     */
    public static Set<String> getSecurePathWhiteList() throws IOException
    {
        // current workspace can't be root directory.
        if (new File(".").getCanonicalPath().equals("/")) {
            throw new IllegalArgumentException("Current workspace can't be root directory," +
                    "please make sure you have config node.data-dir in node.properties or don't put your bin directory under root directory");
        }
        // if user config node.data-dir, then current workspace is node.data-dir; if not, the current workspace is the same as bin.
        // the working directory is automatically set to {INSTALL_DIR}/bin by the launcher
        // the white list includes the {INSTALL_DIR}
        securePathwhiteList.add(new File("..").getCanonicalPath());
        return securePathwhiteList;
    }

    public static boolean isSecurePath(Path absolutePath) throws IOException
    {
        return isSecurePath(absolutePath.toString());
    }

    public static boolean isSecurePath(String absolutePath) throws IOException
    {
        // absolutePath
        if (absolutePath.startsWith("/")) {
            return getSecurePathWhiteList().stream()
                    .filter(securePath -> absolutePath.startsWith(securePath))
                    .findAny()
                    .isPresent();
        }
        // currentDirectory
        else {
            return true;
        }
    }
}
