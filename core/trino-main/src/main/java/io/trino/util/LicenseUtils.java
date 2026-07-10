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
package io.trino.util;

import com.asiainfo.zodiac.AccreditTools;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.server.ServerConfig;
import io.trino.spi.TrinoException;

import java.io.File;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;

import static io.trino.spi.StandardErrorCode.INVALID_PATH;

public class LicenseUtils
{
    private static final Logger LOG = Logger.get(LicenseUtils.class);
    private static String licensePath;

    @Inject
    public LicenseUtils(ServerConfig serverConfig)
    {
        licensePath = serverConfig.getLicensePath();
    }

    public void init() {}

    public static boolean exists()
    {
        LOG.debug("-- license path %s -- ", licensePath);
        if (licensePath == null || licensePath.isEmpty()) {
            throw new TrinoException(INVALID_PATH,
                    "Configuration property 'coordinator.license-path' is required.");
        }
        else {
            File file = new File(licensePath);
            if (file.isDirectory()) {
                throw new TrinoException(INVALID_PATH,
                        "Configuration property 'coordinator.license-path' should end with the license file name.");
            }
            return file.exists();
        }
    }

    public static boolean expire(long time)
    {
        JsonObject license = getLicense();
        long experience = license.get("experience").getAsLong();
        return experience - System.currentTimeMillis() < time;
    }

    public static boolean effective()
    {
        JsonObject license = getLicense();
        long experience = license.get("experience").getAsLong();
        String macAddress = license.get("macAddress").getAsString();
        List<String> addressList = getMacAddress();
        return addressList != null && addressList.contains(macAddress) && experience > System.currentTimeMillis();
    }

    private static JsonObject getLicense()
    {
        File file = new File(licensePath);
        try {
            String license = null;
            license = new AccreditTools().DropMask(file.toURI().toURL().getFile());
            return new JsonParser().parse(license).getAsJsonObject();
        }
        catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static List<String> getMacAddress()
    {
        try {
            Enumeration<NetworkInterface> nifs = NetworkInterface.getNetworkInterfaces();
            List<String> addressList = new ArrayList<>();
            while (nifs.hasMoreElements()) {
                NetworkInterface nif = nifs.nextElement();
                Enumeration<InetAddress> addresses = nif.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    NetworkInterface networkInterface = NetworkInterface.getByInetAddress(addr);
                    byte[] mac = NetworkInterface.getByInetAddress(addr).getHardwareAddress();
                    if (mac != null) {
                        addressList.add(getMacAddress(mac));
                    }
                }
            }
            return addressList;
        }
        catch (SocketException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String getMacAddress(byte[] mac)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < mac.length; i++) {
            if (i != 0) {
                sb.append("-");
            }
            String s = Integer.toHexString(mac[i] & 0xFF);
            sb.append(s.length() == 1 ? 0 + s : s);
        }
        return sb.toString().toUpperCase(Locale.US);
    }
}
