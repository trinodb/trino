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
package io.trino.testing.containers.environment;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Manages product test environments with single-environment-at-a-time constraint.
 * <p>
 * This class ensures that only one environment is running at any time to prevent
 * OOM conditions on CI runners. It supports two modes:
 * <ul>
 *   <li><b>STRICT</b>: Throws an exception if a different environment is requested
 *       while one is running. Use this in CI to catch test ordering issues.</li>
 *   <li><b>LENIENT</b>: Automatically shuts down the current environment and starts
 *       the new one. Use this in IDE for developer convenience.</li>
 * </ul>
 * <p>
 * The mode is controlled by the {@code trino.product-test.environment-mode} system property.
 * Default is LENIENT for developer friendliness.
 * <p>
 * <b>Usage pattern:</b>
 * <pre>{@code
 * // Get the shared instance (preferred for Suite runners and extensions)
 * EnvironmentManager manager = EnvironmentManager.shared();
 *
 * // When a test needs an environment
 * ProductTestEnvironment env = manager.acquire(MySqlEnvironment.class);
 * }</pre>
 *
 * @see #shared()
 */
public final class EnvironmentManager
        implements AutoCloseable
{
    /**
     * Shared singleton instance used by both Suite runners and JUnit extensions.
     * This ensures there's only one EnvironmentManager controlling environment lifecycle,
     * preventing duplicate container issues when suites start environments and JUnit
     * extensions try to start them again.
     */
    private static final EnvironmentManager SHARED = new EnvironmentManager();

    /**
     * Returns the shared EnvironmentManager instance.
     * <p>
     * Use this method instead of creating new instances to ensure proper coordination
     * between Suite runners and JUnit extensions. Both should use the same manager
     * to avoid starting duplicate environments.
     *
     * @return the shared EnvironmentManager instance
     */
    public static EnvironmentManager shared()
    {
        return SHARED;
    }

    public enum Mode
    {
        /**
         * Throw exception if different environment requested while one is running.
         * Recommended for CI to catch test ordering issues early.
         */
        STRICT,

        /**
         * Automatically shutdown current environment and start new one.
         * Recommended for IDE development for convenience.
         */
        LENIENT
    }

    private static final String MODE_PROPERTY = "trino.product-test.environment-mode";

    private final Lock lock = new ReentrantLock();
    private final Mode mode;
    private final AtomicReference<EnvironmentStats> stats = new AtomicReference<>(new EnvironmentStats());

    // Current running environment (null if none)
    private ProductTestEnvironment currentEnvironment;
    private Class<? extends ProductTestEnvironment> currentEnvironmentClass;

    public EnvironmentManager()
    {
        this(Mode.valueOf(System.getProperty(MODE_PROPERTY, Mode.LENIENT.name())));
    }

    public EnvironmentManager(Mode mode)
    {
        this.mode = mode;
    }

    /**
     * Acquires an environment of the specified type. If the same environment type
     * is already running, returns it. Otherwise, behavior depends on the mode:
     * <ul>
     *   <li>STRICT: throws if different environment is running</li>
     *   <li>LENIENT: shuts down current environment and starts the requested one</li>
     * </ul>
     *
     * @param environmentClass the environment class to acquire
     * @return the running environment instance
     * @throws EnvironmentConflictException in STRICT mode when different environment is running
     */
    public <T extends ProductTestEnvironment> T acquire(Class<T> environmentClass)
    {
        lock.lock();
        try {
            // Compatible environment already running? Reuse it.
            // This supports superclass-based environment reuse:
            // if HiveKerberosKmsEnvironment is running, a request for HiveBasicEnvironment
            // can safely reuse it, but not vice-versa.
            if (currentEnvironment != null && environmentClass.isAssignableFrom(currentEnvironmentClass)) {
                stats.updateAndGet(s -> s.withReuse());
                return environmentClass.cast(currentEnvironment);
            }

            // Different environment running - handle based on mode
            if (currentEnvironment != null) {
                if (mode == Mode.STRICT) {
                    throw new EnvironmentConflictException(
                            "Environment conflict: %s is running but %s was requested. " +
                            "In STRICT mode, only one environment type can run at a time. " +
                            "Either run tests for one environment at a time (use Suite classes), " +
                            "or set -D%s=LENIENT for automatic environment switching.",
                            currentEnvironmentClass.getSimpleName(),
                            environmentClass.getSimpleName(),
                            MODE_PROPERTY);
                }

                // LENIENT mode: shut down current, start new
                stats.updateAndGet(s -> s.withSwitch());
                shutdownCurrent();
            }

            // Start the new environment - only update state after successful start
            stats.updateAndGet(s -> s.withStart());
            T newEnvironment = instantiate(environmentClass);
            newEnvironment.setManaged(true);
            newEnvironment.start();  // If this throws, currentEnvironment stays null

            // Only set current after successful start
            currentEnvironment = newEnvironment;
            currentEnvironmentClass = environmentClass;

            return newEnvironment;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Returns the currently running environment, or null if none.
     */
    public ProductTestEnvironment current()
    {
        lock.lock();
        try {
            return currentEnvironment;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Returns the class of the currently running environment, or null if none.
     */
    public Class<? extends ProductTestEnvironment> currentClass()
    {
        lock.lock();
        try {
            return currentEnvironmentClass;
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Explicitly shuts down the current environment. Normally called automatically
     * via {@link #close()}, but can be called manually if needed.
     */
    public void shutdown()
    {
        lock.lock();
        try {
            shutdownCurrent();
        }
        finally {
            lock.unlock();
        }
    }

    private void shutdownCurrent()
    {
        if (currentEnvironment != null) {
            try {
                // Allow the close call and then close
                currentEnvironment.allowClose();
                currentEnvironment.close();
            }
            catch (Exception e) {
                // Log but don't throw - we want cleanup to proceed
                System.err.println("Warning: Error closing environment " +
                        currentEnvironmentClass.getSimpleName() + ": " + e.getMessage());
            }
            currentEnvironment = null;
            currentEnvironmentClass = null;
        }
    }

    /**
     * Returns statistics about environment usage during this test run.
     * Useful for detecting inefficient test ordering (many switches).
     */
    public EnvironmentStats getStats()
    {
        return stats.get();
    }

    @Override
    public void close()
    {
        shutdown();

        // Print stats summary if there were environment switches (potential inefficiency)
        EnvironmentStats finalStats = stats.get();
        if (finalStats.switches() > 0) {
            System.out.println("\n=== Product Test Environment Stats ===");
            System.out.println("Environment starts:   " + finalStats.starts());
            System.out.println("Environment reuses:   " + finalStats.reuses());
            System.out.println("Environment switches: " + finalStats.switches());
            if (finalStats.switches() > 2) {
                System.out.println("WARNING: High number of environment switches detected.");
                System.out.println("Consider grouping tests by environment for better performance.");
            }
            System.out.println("======================================\n");
        }
    }

    private static <T extends ProductTestEnvironment> T instantiate(Class<T> environmentClass)
    {
        try {
            return environmentClass.getDeclaredConstructor().newInstance();
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to instantiate environment: " + environmentClass.getName() +
                    ". Ensure it has a public no-arg constructor.", e);
        }
    }

    /**
     * Statistics about environment usage during a test run.
     */
    public record EnvironmentStats(int starts, int reuses, int switches)
    {
        public EnvironmentStats()
        {
            this(0, 0, 0);
        }

        EnvironmentStats withStart()
        {
            return new EnvironmentStats(starts + 1, reuses, switches);
        }

        EnvironmentStats withReuse()
        {
            return new EnvironmentStats(starts, reuses + 1, switches);
        }

        EnvironmentStats withSwitch()
        {
            return new EnvironmentStats(starts, reuses, switches + 1);
        }
    }
}
