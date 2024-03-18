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
package io.trino.plugin.varada.dispatcher.query.classifier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.cache.DispatcherCacheTransformer;
import io.trino.plugin.varada.dispatcher.query.MatchCollectIdService;
import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.juffer.PredicatesCacheService;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;

import java.util.List;

import static java.util.Objects.requireNonNull;

@Singleton
public class ClassifierFactory
{
    private final StorageEngineConstants storageEngineConstants;
    private final PredicatesCacheService predicatesCacheService;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final MatchCollectIdService matchCollectIdService;
    private final GlobalConfiguration globalConfiguration;
    private final NativeConfiguration nativeConfiguration;
    private final BufferAllocator bufferAllocator;
    private ImmutableMap<ClassificationType, List<Classifier>> classificationTypeToClassifiers;
    private ImmutableMap<ClassificationType, DispatcherProxiedConnectorTransformer> classificationTypeToConnectorTransformer;

    @Inject
    public ClassifierFactory(StorageEngineConstants storageEngineConstants,
            PredicatesCacheService predicatesCacheService,
            BufferAllocator bufferAllocator,
            NativeConfiguration nativeConfiguration,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            MatchCollectIdService matchCollectIdService,
            GlobalConfiguration globalConfiguration)
    {
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
        this.predicatesCacheService = requireNonNull(predicatesCacheService);
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.nativeConfiguration = requireNonNull(nativeConfiguration);
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.matchCollectIdService = requireNonNull(matchCollectIdService);
        this.globalConfiguration = requireNonNull(globalConfiguration);
    }

    private void buildClassifiers()
    {
        ImmutableMap.Builder<ClassificationType, List<Classifier>> classificationTypeToClassifiersBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<ClassificationType, DispatcherProxiedConnectorTransformer> classificationTypeToConnectorTransformerBuilder = ImmutableMap.builder();
        MatchClassifier matchClassifier = getMatchClassifier();
        PrefilledCollectClassifier prefilledCollectClassifier = new PrefilledCollectClassifier(
                dispatcherProxiedConnectorTransformer,
                globalConfiguration);
        NativeCollectClassifier nativeCollectClassifier = new NativeCollectClassifier(
                storageEngineConstants.getMatchCollectBufferSize(),
                storageEngineConstants.getMaxChunksInRange(),
                nativeConfiguration.getBundleSize() - storageEngineConstants.getBundleNonCollectSize(),
                nativeConfiguration.getCollectTxSize(),
                storageEngineConstants.getMatchTxSize(),
                storageEngineConstants.getMaxMatchColumns(),
                bufferAllocator,
                dispatcherProxiedConnectorTransformer);
        DispatcherCacheTransformer dispatcherCacheTransformer = new DispatcherCacheTransformer();
        NativeCollectClassifier nativeCacheCollectClassifier = new NativeCollectClassifier(
                storageEngineConstants.getMatchCollectBufferSize(),
                storageEngineConstants.getMaxChunksInRange(),
                nativeConfiguration.getBundleSize() - storageEngineConstants.getBundleNonCollectSize(),
                nativeConfiguration.getCollectTxSize(),
                storageEngineConstants.getMatchTxSize(),
                storageEngineConstants.getMaxMatchColumns(),
                bufferAllocator,
                dispatcherCacheTransformer);
        MatchPrepareAfterCollectClassifier matchPrepareAfterCollectClassifier = new MatchPrepareAfterCollectClassifier(matchCollectIdService,
                storageEngineConstants.getMaxMatchColumns());
        PredicateBufferClassifier predicateBufferClassifier = new PredicateBufferClassifier(predicatesCacheService);
        AllProxyDecisionClassifier allProxyDecisionClassifier = new AllProxyDecisionClassifier(dispatcherProxiedConnectorTransformer);
        List<Classifier> classifiers = List.of(
                matchClassifier,
                prefilledCollectClassifier,
                nativeCollectClassifier,
                matchPrepareAfterCollectClassifier,
                predicateBufferClassifier,
                allProxyDecisionClassifier);
        classificationTypeToClassifiersBuilder.put(ClassificationType.QUERY, classifiers);
        classificationTypeToClassifiersBuilder.put(ClassificationType.CHOOSING_ALTERNATIVE, classifiers);
        // these classifiers used by warming flow to decide what to warm.
        // must make sure that any classifier that allocate resources should not be part of this list (E.g. predicateBufferClassifier)
        List<Classifier> warmingClassifiers = List.of(
                matchClassifier,
                prefilledCollectClassifier,
                nativeCollectClassifier,
                matchPrepareAfterCollectClassifier,
                allProxyDecisionClassifier);
        classificationTypeToClassifiersBuilder.put(ClassificationType.WARMING, warmingClassifiers);
        classificationTypeToClassifiersBuilder.put(ClassificationType.CACHE, List.of(nativeCacheCollectClassifier));
        classificationTypeToConnectorTransformerBuilder.put(ClassificationType.QUERY, dispatcherProxiedConnectorTransformer);
        classificationTypeToConnectorTransformerBuilder.put(ClassificationType.CHOOSING_ALTERNATIVE, dispatcherProxiedConnectorTransformer);
        classificationTypeToConnectorTransformerBuilder.put(ClassificationType.CACHE, dispatcherCacheTransformer);
        this.classificationTypeToConnectorTransformer = classificationTypeToConnectorTransformerBuilder.buildOrThrow();
        this.classificationTypeToClassifiers = classificationTypeToClassifiersBuilder.buildOrThrow();
    }

    private MatchClassifier getMatchClassifier()
    {
        ImmutableList.Builder<Matcher> matchers = ImmutableList.builder();
        if (globalConfiguration.getEnableRangeFilter()) {
            matchers.add(new RangeMatcher(globalConfiguration));
        }
        matchers.add(new BloomMatcher(),
                new LuceneElementsMatcher(dispatcherProxiedConnectorTransformer),
                new BasicMatcher(),
                new RemoveBloomFromRemainingMatcher());
        return new MatchClassifier(matchers.build(), globalConfiguration);
    }

    List<Classifier> getClassifiers(ClassificationType classificationType)
    {
        if (classificationTypeToClassifiers == null) {
            buildClassifiers();
        }
        return classificationTypeToClassifiers.get(classificationType);
    }

    DispatcherProxiedConnectorTransformer getTransformerByType(ClassificationType classificationType)
    {
        if (classificationTypeToConnectorTransformer == null) {
            buildClassifiers();
        }
        return classificationTypeToConnectorTransformer.get(classificationType);
    }
}
