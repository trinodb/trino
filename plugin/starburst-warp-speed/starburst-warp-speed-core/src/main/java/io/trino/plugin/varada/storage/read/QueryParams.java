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
package io.trino.plugin.varada.storage.read;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.varada.juffer.PredicateCacheData;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static io.trino.plugin.warp.gen.constants.WECollectJparams.WE_COLLECT_JPARAMS_NUM_OF;
import static java.util.Objects.requireNonNull;

public class QueryParams
{
    private final Optional<MatchNode> rootMatchNode;
    private final List<WarmupElementMatchParams> leaves;
    private final int numLucene;
    private final List<WarmupElementCollectParams> collectParams;
    private final int numLoadDataValues;
    private final int numMatchCollect;
    private final int totalNumRecords;
    private final int catalogSequence;
    private final int minMatchOffset;
    private final int minCollectOffset;
    private final ImmutableList<PredicateCacheData> predicateCacheData;

    private final String filePath;
    private final long rowGroupUniqueId; // TBD - will be used for logs, currently zero

    public QueryParams(Optional<MatchNode> rootMatchNode,
            int numLucene,
            List<WarmupElementCollectParams> collectParams,
            int totalNumRecords,
            int catalogSequence,
            int minMatchOffset,
            int minCollectOffset,
            String filePath,
            ImmutableList<PredicateCacheData> predicateCacheData)
    {
        this.rootMatchNode = requireNonNull(rootMatchNode, "rootMatchNode is null");
        this.leaves = rootMatchNode.map(this::getLeaves).orElse(Collections.emptyList());
        this.numLucene = numLucene;
        this.collectParams = requireNonNull(collectParams, "collectParams is null");
        this.numLoadDataValues = (int) collectParams.stream()
                .filter(WarmupElementCollectParams::hasDictionaryParams)
                .count();
        this.numMatchCollect = (int) collectParams.stream()
                .filter(WarmupElementCollectParams::hasMatchCollect)
                .count();
        this.totalNumRecords = totalNumRecords;
        this.catalogSequence = catalogSequence;
        this.minMatchOffset = minMatchOffset;
        this.minCollectOffset = minCollectOffset;
        this.predicateCacheData = predicateCacheData;
        //TODO hash of file path
        this.rowGroupUniqueId = Calendar.getInstance().getTimeInMillis();
        this.filePath = filePath;
    }

    private List<WarmupElementMatchParams> getLeaves(MatchNode node)
    {
        if (node instanceof WarmupElementMatchParams) {
            return List.of((WarmupElementMatchParams) node);
        }
        List<WarmupElementMatchParams> res = new ArrayList<>();
        for (MatchNode child : node.getChildren()) {
            if (child instanceof WarmupElementMatchParams) {
                res.add((WarmupElementMatchParams) child);
            }
            else {
                res.addAll(getLeaves(child));
            }
        }
        return res;
    }

    public List<WarmupElementMatchParams> getMatchElementsParamsList()
    {
        return leaves;
    }

    public List<WarmupElementCollectParams> getCollectElementsParamsList()
    {
        return collectParams;
    }

    public int getNumMatchElements()
    {
        return leaves.size();
    }

    public int getNumCollectElements()
    {
        return collectParams.size();
    }

    public int getNumLucene()
    {
        return numLucene;
    }

    public int getNumLoadDataValues()
    {
        return numLoadDataValues;
    }

    public int getNumMatchCollect()
    {
        return numMatchCollect;
    }

    public int getTotalNumRecords()
    {
        return totalNumRecords;
    }

    public String getFilePath()
    {
        return filePath;
    }

    public int getCatalogSequence()
    {
        return catalogSequence;
    }

    public int getMinMatchOffset()
    {
        return minMatchOffset;
    }

    public int getMinCollectOffset()
    {
        return minCollectOffset;
    }

    public int[] dumpMatchParams()
    {
        if (rootMatchNode.isEmpty()) {
            return new int[0];
        }
        int[] dump = new int[rootMatchNode.get().getDumpSize()];
        rootMatchNode.get().dump(dump, 0);
        return dump;
    }

    public int[] dumpCollectParams()
    {
        int[] dump = new int[WE_COLLECT_JPARAMS_NUM_OF.ordinal() * collectParams.size()];
        int collectOffset = 0;
        for (WarmupElementCollectParams collectParams : collectParams) {
            // in case the record length we put in the output page is larger than the one used for warming (dictionary case),
            // storage engine needs it to calculate the number of records passed in a single call to match and collect
            // it must be passed in txQueryCreate to allow native to prepare the metadata once in advance and not on every call to collect
            collectParams.dump(dump, collectOffset);
            collectOffset += WE_COLLECT_JPARAMS_NUM_OF.ordinal();
        }
        return dump;
    }

    public ImmutableList<PredicateCacheData> getPredicateCacheData()
    {
        return predicateCacheData;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(rootMatchNode, collectParams, rowGroupUniqueId);
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof QueryParams o)) {
            return false;
        }

        return rootMatchNode.equals(o.rootMatchNode) &&
                collectParams.equals(o.collectParams) &&
                catalogSequence == o.catalogSequence &&
                rowGroupUniqueId == o.rowGroupUniqueId;
    }

    @Override
    public String toString()
    {
        return String.format("rootMatchNode %s collectParamsList %s numLoadDataValues %d numLucene %d",
                rootMatchNode, collectParams, numLoadDataValues, numLucene);
    }

    public Optional<MatchNode> getRootMatchNode()
    {
        return rootMatchNode;
    }
}
