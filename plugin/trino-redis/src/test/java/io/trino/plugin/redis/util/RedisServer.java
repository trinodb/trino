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
package io.trino.plugin.redis.util;

import com.google.common.net.HostAndPort;
import org.assertj.core.util.Files;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import redis.clients.jedis.JedisPool;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

public class RedisServer
        implements Closeable
{
    public static final String DEFAULT_VERSION = "2.8.9";
    public static final String LATEST_VERSION = "7.0.0";
    private static final int PORT = 6379;

    public static final String USER = "test";
    public static final String PASSWORD = "password";

    private final GenericContainer<?> container;
    private final File containerFilesDir;
    private final JedisPool jedisPool;

    public RedisServer() throws UnrecoverableKeyException, KeyStoreException, NoSuchAlgorithmException, KeyManagementException, CertificateException, IOException
    {
        this(DEFAULT_VERSION, RedisSecurityFeature.NONE);
    }

    public RedisServer(String version, RedisSecurityFeature feature) throws KeyStoreException, NoSuchAlgorithmException, UnrecoverableKeyException, KeyManagementException, IOException, CertificateException
    {
        containerFilesDir = Files.newTemporaryFolder();
        containerFilesDir.deleteOnExit();

        switch (feature) {
            case USER_PASSWORD -> {
                container = new GenericContainer<>("redis:" + version)
                        .withCommand("redis-server", "--requirepass", PASSWORD)
                        .withExposedPorts(PORT);
                container.start();
                jedisPool = new JedisPool(container.getHost(), container.getMappedPort(PORT), null, PASSWORD);
                jedisPool.getResource().aclSetUser(USER, "on", ">" + PASSWORD, "~*:*", "+@all");
            }
            case TLS -> {
                container = new GenericContainer(
                        new ImageFromDockerfile("redistls", false)
                            .withFileFromClasspath("./gen-test-certs.sh", "docker/gen-test-certs.sh")
                            .withFileFromClasspath("Dockerfile", "docker/Dockerfile"))
                        .withExposedPorts(PORT)
                        .withCommand(
                                "redis-server",
                                "--tls-port", Integer.toString(PORT),
                                "--port", "0",
                                "--tls-cert-file", "/tests/tls/redis.crt",
                                "--tls-key-file", "/tests/tls/redis.key",
                                "--tls-ca-cert-file", "/tests/tls/ca.crt");
                container.start();

                var keyStore = KeyStore.getInstance("pkcs12");
                var keyStoreFile = containerFilesDir.toPath().resolve("redis.p12");
                container.copyFileFromContainer("/tests/tls/redis.p12", keyStoreFile.toString());
                keyStore.load(new FileInputStream(keyStoreFile.toString()), "secret".toCharArray());

                var keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keyManagerFactory.init(keyStore, "secret".toCharArray());

                var trustStore = KeyStore.getInstance("pkcs12");
                var trustStoreFile = containerFilesDir.toPath().resolve("ca.p12");
                container.copyFileFromContainer("/tests/tls/ca.p12", trustStoreFile.toString());
                trustStore.load(new FileInputStream(trustStoreFile.toString()), "secret".toCharArray());

                var trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(trustStore);

                var sslContext = SSLContext.getInstance("TLS");
                sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

                jedisPool = new JedisPool(container.getHost(), container.getMappedPort(PORT), true, sslContext.getSocketFactory(), sslContext.getDefaultSSLParameters(), null);
            }
            default -> {
                container = new GenericContainer<>("redis:" + version).withExposedPorts(PORT);
                container.start();
                jedisPool = new JedisPool(container.getHost(), container.getMappedPort(PORT));
            }
        }
    }

    public File getContainerFilesDir()
    {
        return containerFilesDir;
    }

    public JedisPool getJedisPool()
    {
        return jedisPool;
    }

    public void destroyJedisPool()
    {
        jedisPool.destroy();
    }

    public HostAndPort getHostAndPort()
    {
        return HostAndPort.fromParts(container.getHost(), container.getMappedPort(PORT));
    }

    @Override
    public void close()
    {
        jedisPool.destroy();
        container.close();
    }
}
