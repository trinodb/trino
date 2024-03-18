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

import io.airlift.log.Logger;
import io.trino.plugin.varada.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.varada.dispatcher.query.QueryContext;

import java.util.Collections;

class AllProxyDecisionClassifier
        implements Classifier
{
    private static final Logger logger = Logger.get(AllProxyDecisionClassifier.class);

    private final DispatcherProxiedConnectorTransformer transformer;

    public AllProxyDecisionClassifier(DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        this.transformer = dispatcherProxiedConnectorTransformer;
    }

    @Override
    public QueryContext classify(ClassifyArgs classifyArgs, QueryContext queryContext)
    {
        if (queryContext.getMatchData().isEmpty() &&
                queryContext.getPredicateContextData().getRemainingColumns().size() > 0 &&
                transformer.proxyHasPushedDownFilter(classifyArgs.getDispatcherTableHandle()) &&
                !queryContext.getRemainingCollectColumnByBlockIndex().isEmpty()) {
            // Since there are no matches in Varada while there are matches in proxy, and we must go to proxy for collect,
            // if the proxy has pushdowns go all proxy - and try to benefit from proxy's limited filtering capabilities.
            logger.debug("Classified as all proxy");
            return queryContext.asBuilder()
                    .nativeQueryCollectDataList(Collections.emptyList())
                    .prefilledQueryCollectDataByBlockIndex(Collections.emptyMap())
                    .remainingCollectColumnByBlockIndex(classifyArgs.getCollectColumnsByBlockIndex())
                    .build();
        }

        return queryContext;
    }
}
