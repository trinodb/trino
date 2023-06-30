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
package org.apache.phoenix.shaded.org.apache.zookeeper.client;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

// TODO(https://github.com/trinodb/trino/issues/13051): Remove when Phoenix 5.2 is release
public final class StaticHostProvider
        implements HostProvider
{
    public interface Resolver
    {
        InetAddress[] getAllByName(String name)
                throws UnknownHostException;
    }

    private final List<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>(5);

    private int lastIndex = -1;

    private int currentIndex = -1;

    private Resolver resolver;

    /**
     * Constructs a SimpleHostSet.
     *
     * @param serverAddresses
     *            possibly unresolved ZooKeeper server addresses
     * @throws IllegalArgumentException
     *             if serverAddresses is empty or resolves to an empty list
     */
    public StaticHostProvider(Collection<InetSocketAddress> serverAddresses)
    {
        this.resolver = name -> InetAddress.getAllByName(name);
        init(serverAddresses);
    }

    /**
     * Introduced for testing purposes. getAllByName() is a static method of InetAddress, therefore cannot be easily mocked.
     * By abstraction of Resolver interface we can easily inject a mocked implementation in tests.
     *
     * @param serverAddresses
     *            possibly unresolved ZooKeeper server addresses
     * @param resolver
     *            custom resolver implementation
     * @throws IllegalArgumentException
     *             if serverAddresses is empty or resolves to an empty list
     */
    public StaticHostProvider(Collection<InetSocketAddress> serverAddresses, Resolver resolver)
    {
        this.resolver = resolver;
        init(serverAddresses);
    }

    /**
     * Common init method for all constructors.
     * Resolve all unresolved server addresses, put them in a list and shuffle.
     */
    private void init(Collection<InetSocketAddress> serverAddresses)
    {
        if (serverAddresses.isEmpty()) {
            throw new IllegalArgumentException(
                    "A HostProvider may not be empty!");
        }

        this.serverAddresses.addAll(serverAddresses);
        Collections.shuffle(this.serverAddresses);
    }

    /**
     * Evaluate to a hostname if one is available and otherwise it returns the
     * string representation of the IP address.
     *
     * In Java 7, we have a method getHostString, but earlier versions do not support it.
     * This method is to provide a replacement for InetSocketAddress.getHostString().
     *
     * @return Hostname string of address parameter
     */
    private String getHostString(InetSocketAddress addr)
    {
        String hostString = "";

        if (addr == null) {
            return hostString;
        }
        if (!addr.isUnresolved()) {
            InetAddress ia = addr.getAddress();

            // If the string starts with '/', then it has no hostname
            // and we want to avoid the reverse lookup, so we return
            // the string representation of the address.
            if (ia.toString().startsWith("/")) {
                hostString = ia.getHostAddress();
            }
            else {
                hostString = addr.getHostName();
            }
        }
        else {
            hostString = addr.getHostString();
        }

        return hostString;
    }

    @Override
    public int size()
    {
        return serverAddresses.size();
    }

    @Override
    public InetSocketAddress next(long spinDelay)
    {
        currentIndex = ++currentIndex % serverAddresses.size();
        if (currentIndex == lastIndex && spinDelay > 0) {
            try {
                Thread.sleep(spinDelay);
            }
            catch (InterruptedException e) {
            }
        }
        else if (lastIndex == -1) {
            // We don't want to sleep on the first ever connect attempt.
            lastIndex = 0;
        }

        InetSocketAddress curAddr = serverAddresses.get(currentIndex);
        try {
            String curHostString = getHostString(curAddr);
            List<InetAddress> resolvedAddresses = new ArrayList<InetAddress>(Arrays.asList(this.resolver.getAllByName(curHostString)));
            if (resolvedAddresses.isEmpty()) {
                return curAddr;
            }
            Collections.shuffle(resolvedAddresses);
            return new InetSocketAddress(resolvedAddresses.get(0), curAddr.getPort());
        }
        catch (UnknownHostException e) {
            return curAddr;
        }
    }

    @Override
    public void onConnected()
    {
        lastIndex = currentIndex;
    }
}
