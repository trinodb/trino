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
package io.trino.metadata;

import com.google.common.io.Files;
import io.airlift.http.client.Request;
import io.trino.execution.FunctionsReader;
import io.trino.server.FunctionJarResource;
import io.trino.spi.Plugin;
import io.trino.spi.TrinoException;

import javax.inject.Inject;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.trino.metadata.FunctionJarDynamicManager.JarAction.ADD;
import static io.trino.metadata.FunctionJarDynamicManager.JarAction.DROP;
import static io.trino.spi.StandardErrorCode.FUNCTION_REGISTER_ERROR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class FunctionJarDynamicManager
{
    private final FunctionJarStore functionJarStore;
    private final GlobalFunctionCatalog globalFunctionCatalog;
    private final DiscoveryNodeManager nodeManager;

    @Inject
    public FunctionJarDynamicManager(GlobalFunctionCatalog globalFunctionCatalog,
            DiscoveryNodeManager nodeManager,
            FunctionJarStore functionJarStore)
    {
        this.globalFunctionCatalog = globalFunctionCatalog;
        this.nodeManager = nodeManager;
        this.functionJarStore = functionJarStore;
    }

    public void addJar(String jarUrl, boolean notExists)
    {
        addJar(jarUrl, notExists, true);
    }

    public void addJar(String jarUrl, boolean notExists, boolean needRestore)
    {
        File jarFile = new File(jarUrl); // ignore_security_alert
        String jarName = getJarNameBasedType(jarUrl);
        boolean hasRegistered = functionJarStore.exists(jarName);
        // udfMap and globalCatalog function's status is inconsistent, we will add jar ignore considering hasRegister
        if (!notExists && hasRegistered) {
            throw new TrinoException(FUNCTION_REGISTER_ERROR, String.format("udf jar already registered: %s", jarFile.getAbsolutePath()));
        }

        // TODO transaction between functions and udfJarMap
        // first worker then coordinator.
        // assure that simple 2-phase phase
        boolean workerRegistered = proxyJarOnWorker(jarUrl, ADD, notExists);
        if (!notExists && !workerRegistered) {
            throw new TrinoException(FUNCTION_REGISTER_ERROR, String.format("udf jar registered error on worker: %s", jarFile.getAbsolutePath()));
        }

        List<Plugin> plugins = FunctionsReader.loadFunctions(jarFile);
        plugins.forEach(plugin -> {
            Set<Class<?>> functions = plugin.getFunctions();
            if (!functions.isEmpty()) {
                InternalFunctionBundle.InternalFunctionBundleBuilder builder = InternalFunctionBundle.builder();
                functions.forEach(v -> builder.functions(v, jarName, jarUrl));
                InternalFunctionBundle functionBundle = builder.build();
                globalFunctionCatalog.addFunctions(functionBundle);
                // restart will not refresh
                if (needRestore) {
                    functionJarStore.addJar(
                            new FunctionJar(jarUrl, functionBundle.getFunctions().stream().map(v -> v.getFunctionId().getId()).collect(toImmutableList()), false));
                }
            }
        });
    }

    public enum JarAction
    {
        ADD,
        DROP
    }

    private boolean proxyJarOnWorker(String jarUrl, JarAction action, Boolean exists)
    {
        // @VisibleForTesting
        if (nodeManager == null) {
            return true;
        }
        requireNonNull(jarUrl, "jarUrl is null");
        Set<InternalNode> activeNodes = nodeManager.getNodes(NodeState.ACTIVE);
        InternalNode currentNode = nodeManager.getCurrentNode();
        if (!currentNode.isCoordinator()) {
            return true;
        }
        Set<InternalNode> otherNodes = activeNodes.stream().filter(v -> !v.equals(currentNode)).collect(Collectors.toSet());

        otherNodes.forEach(node -> {
            Request request;
            try {
                Request.Builder builder = ADD.equals(action) ? preparePost() : prepareDelete();
                request = builder.setUri(uriBuilderFrom(node.getInternalUri())
                                .appendPath("/v1/jar")
                                .appendPath(URLEncoder.encode(jarUrl, "UTF-8"))
                                .appendPath(exists.toString())
                                .build())
                        .build();
            }
            catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }

            nodeManager.getHttpClient().execute(request, new FunctionJarResource.StreamingJsonResponseHandler());
        });

        return true;
    }

    public void dropJar(String jarUrl, boolean exists)
    {
        String jarName = getJarNameBasedType(jarUrl); // ignore_security_alert
        boolean hasRegistered = functionJarStore.exists(jarName);
        // if having no registered, but have
        if (!exists && !hasRegistered) {
            throw new TrinoException(FUNCTION_REGISTER_ERROR, String.format("udf jar has not registered: %s", jarUrl));
        }

        // first worker then coordinator.
        // assure that simple 2-phase phase
        boolean workerRegistered = proxyJarOnWorker(jarUrl, DROP, exists);
        if (!exists && !workerRegistered) {
            throw new TrinoException(FUNCTION_REGISTER_ERROR, String.format("udf jar registered error on worker: %s", jarUrl));
        }

        if (!hasRegistered) {
            return;
        }

        functionJarStore.dropJar(jarName, globalFunctionCatalog);
    }

    /**
     * TODO  Distinguish fileSystem based jar's url, here only use local file
     *
     * @param jarFile
     * @return
     */
    public static String getJarNameBasedType(String jarFile)
    {
        File jar = new File(jarFile); // ignore_security_alert
        if ((jar.exists() && (jar.isDirectory() || jar.isFile()))) {
            return Files.getNameWithoutExtension(jarFile).toLowerCase(ENGLISH);
        }
        else {
            return jarFile;
        }
    }

    public FunctionJarStore getFunctionJarStore()
    {
        return functionJarStore;
    }
}
