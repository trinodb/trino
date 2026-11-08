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
package io.trino.cli;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Connects using a JDBC driver other than Trino's, either from the class path or loaded from the given JAR files.
 */
class DriverConnectionFactory
        implements ConnectionFactory
{
    private final Driver driver;
    private final Optional<URLClassLoader> driverClassLoader;
    private final String url;
    private final Properties properties;
    private final Optional<String> catalog;
    private final Optional<String> schema;

    private DriverConnectionFactory(Driver driver, Optional<URLClassLoader> driverClassLoader, String url, Properties properties, Optional<String> catalog, Optional<String> schema)
    {
        this.driver = requireNonNull(driver, "driver is null");
        this.driverClassLoader = requireNonNull(driverClassLoader, "driverClassLoader is null");
        this.url = requireNonNull(url, "url is null");
        this.properties = new Properties();
        this.properties.putAll(requireNonNull(properties, "properties is null"));
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
    }

    public static Builder builder(String url)
    {
        return new Builder(url);
    }

    @Override
    public Connection openConnection()
            throws SQLException
    {
        Connection connection = connect();
        try {
            if (catalog.isPresent()) {
                connection.setCatalog(catalog.get());
            }
            if (schema.isPresent()) {
                connection.setSchema(schema.get());
            }
        }
        catch (SQLException | RuntimeException e) {
            try (connection) {
                throw e;
            }
        }
        return connection;
    }

    @Override
    public void close()
    {
        if (driverClassLoader.isEmpty()) {
            return;
        }
        try {
            driverClassLoader.get().close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Connection connect()
            throws SQLException
    {
        Thread thread = Thread.currentThread();
        ClassLoader originalClassLoader = thread.getContextClassLoader();
        // some drivers load their resources using the context class loader
        driverClassLoader.ifPresent(thread::setContextClassLoader);
        try {
            Connection connection = driver.connect(url, properties);
            if (connection == null) {
                throw new SQLException(format("Driver %s does not accept URL %s", driver.getClass().getName(), url));
            }
            return connection;
        }
        finally {
            thread.setContextClassLoader(originalClassLoader);
        }
    }

    public static final class Builder
    {
        private final String url;
        private Properties properties = new Properties();
        private Optional<String> catalog = Optional.empty();
        private Optional<String> schema = Optional.empty();
        private List<Path> driverPaths = ImmutableList.of();
        private Optional<String> driverClass = Optional.empty();

        private Builder(String url)
        {
            this.url = requireNonNull(url, "url is null");
        }

        public Builder setProperties(Properties properties)
        {
            this.properties = requireNonNull(properties, "properties is null");
            return this;
        }

        public Builder setCatalog(Optional<String> catalog)
        {
            this.catalog = requireNonNull(catalog, "catalog is null");
            return this;
        }

        public Builder setSchema(Optional<String> schema)
        {
            this.schema = requireNonNull(schema, "schema is null");
            return this;
        }

        /**
         * JAR files, or directories with JAR files, with the driver and its dependencies.
         */
        public Builder setDriverPaths(List<Path> driverPaths)
        {
            this.driverPaths = ImmutableList.copyOf(requireNonNull(driverPaths, "driverPaths is null"));
            return this;
        }

        /**
         * Name of the driver class, for drivers that are not registered as a {@link Driver} service.
         */
        public Builder setDriverClass(Optional<String> driverClass)
        {
            this.driverClass = requireNonNull(driverClass, "driverClass is null");
            return this;
        }

        public DriverConnectionFactory build()
                throws SQLException
        {
            Optional<URLClassLoader> driverClassLoader = Optional.empty();
            if (!driverPaths.isEmpty()) {
                driverClassLoader = Optional.of(new URLClassLoader(toUrls(driverPaths), DriverConnectionFactory.class.getClassLoader()));
            }
            ClassLoader classLoader = driverClassLoader.map(ClassLoader.class::cast).orElse(DriverConnectionFactory.class.getClassLoader());
            try {
                Driver driver = findDriver(classLoader);
                return new DriverConnectionFactory(driver, driverClassLoader, url, properties, catalog, schema);
            }
            catch (SQLException | RuntimeException e) {
                driverClassLoader.ifPresent(loader -> closeOnFailure(loader, e));
                throw e;
            }
        }

        private static void closeOnFailure(URLClassLoader classLoader, Exception failure)
        {
            try {
                classLoader.close();
            }
            catch (IOException e) {
                failure.addSuppressed(e);
            }
        }

        private Driver findDriver(ClassLoader classLoader)
                throws SQLException
        {
            if (driverClass.isPresent()) {
                return instantiateDriver(classLoader, driverClass.get());
            }
            // DriverManager only returns drivers loaded by the class loader of the caller, so look for the others directly
            for (ServiceLoader.Provider<Driver> provider : ServiceLoader.load(Driver.class, classLoader).stream().collect(toImmutableList())) {
                try {
                    Driver driver = provider.get();
                    if (driver.acceptsURL(url)) {
                        return driver;
                    }
                }
                catch (SQLException | RuntimeException | ServiceConfigurationError | LinkageError e) {
                    // a broken driver on the path must not prevent finding the right one
                }
            }
            try {
                return DriverManager.getDriver(url);
            }
            catch (SQLException e) {
                throw new SQLException(format("No JDBC driver found for URL %s, use the --driver-path option to load it", url), e);
            }
        }

        private static Driver instantiateDriver(ClassLoader classLoader, String className)
                throws SQLException
        {
            try {
                Class<?> driverClass = Class.forName(className, true, classLoader);
                return (Driver) driverClass.getConstructor().newInstance();
            }
            catch (ReflectiveOperationException | ClassCastException | LinkageError e) {
                throw new SQLException(format("Cannot load JDBC driver class %s: %s", className, e), e);
            }
        }

        private static URL[] toUrls(List<Path> driverPaths)
        {
            return driverPaths.stream()
                    .flatMap(Builder::listJars)
                    .map(Builder::toUrl)
                    .toArray(URL[]::new);
        }

        private static Stream<Path> listJars(Path path)
        {
            if (!Files.isDirectory(path)) {
                if (!Files.isRegularFile(path)) {
                    throw new IllegalArgumentException("Driver path does not exist: " + path);
                }
                return Stream.of(path);
            }
            try (Stream<Path> files = Files.list(path)) {
                List<Path> jars = files
                        .filter(file -> file.getFileName().toString().endsWith(".jar"))
                        .sorted()
                        .collect(toImmutableList());
                if (jars.isEmpty()) {
                    throw new IllegalArgumentException("Driver path does not contain any JAR files: " + path);
                }
                return jars.stream();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static URL toUrl(Path path)
        {
            try {
                return path.toUri().toURL();
            }
            catch (MalformedURLException e) {
                throw new IllegalArgumentException("Invalid driver path: " + path, e);
            }
        }
    }
}
