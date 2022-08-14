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
package io.trino.hdfs;

import io.airlift.log.Logger;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.newSetFromMap;
import static java.util.Objects.requireNonNull;

public class FileSystemFinalizerService
{
    private static final Logger log = Logger.get(FileSystemFinalizerService.class);

    private static Optional<FileSystemFinalizerService> instance = Optional.empty();

    private final Set<FinalizerReference> finalizers = newSetFromMap(new ConcurrentHashMap<>());
    private final ReferenceQueue<Object> finalizerQueue = new ReferenceQueue<>();
    private Thread finalizerThread;

    private FileSystemFinalizerService() {}

    public static synchronized FileSystemFinalizerService getInstance()
    {
        if (instance.isEmpty()) {
            FileSystemFinalizerService finalizer = new FileSystemFinalizerService();
            finalizer.start();
            instance = Optional.of(finalizer);
        }
        return instance.get();
    }

    private void start()
    {
        if (finalizerThread != null) {
            return;
        }
        finalizerThread = new Thread(this::processFinalizerQueue);
        finalizerThread.setDaemon(true);
        finalizerThread.setName("FileSystemFinalizerService");
        finalizerThread.setUncaughtExceptionHandler((thread, e) -> log.error(e, "Uncaught exception in finalizer thread"));
        finalizerThread.start();
    }

    /**
     * When referent is freed by the garbage collector, run cleanup.
     * <p>
     * Note: cleanup must not contain a reference to the referent object.
     */
    public void addFinalizer(Object referent, Runnable cleanup)
    {
        requireNonNull(referent, "referent is null");
        requireNonNull(cleanup, "cleanup is null");
        finalizers.add(new FinalizerReference(referent, finalizerQueue, cleanup));
    }

    private void processFinalizerQueue()
    {
        while (!Thread.interrupted()) {
            try {
                FinalizerReference finalizer = (FinalizerReference) finalizerQueue.remove();
                finalizers.remove(finalizer);
                finalizer.cleanup();
            }
            catch (InterruptedException e) {
                return;
            }
            catch (Throwable t) {
                log.error(t, "Finalizer cleanup failed");
            }
        }
    }

    private static class FinalizerReference
            extends PhantomReference<Object>
    {
        private final Runnable cleanup;
        private final AtomicBoolean executed = new AtomicBoolean();

        public FinalizerReference(Object referent, ReferenceQueue<Object> queue, Runnable cleanup)
        {
            super(requireNonNull(referent, "referent is null"), requireNonNull(queue, "queue is null"));
            this.cleanup = requireNonNull(cleanup, "cleanup is null");
        }

        public void cleanup()
        {
            if (executed.compareAndSet(false, true)) {
                cleanup.run();
            }
        }
    }
}
