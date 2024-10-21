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
package io.trino.plugin.oracle;

import java.util.ArrayList;
import java.util.List;

public class SplittingRule
{
    List<Rule> rules = new ArrayList<>();
    int stride;

    public enum RuleType {
        ROWID,
        INDEX,
        NTILE
    }

    public static class Rule
    {
        String tableName;
        RuleType ruleType;
        int partitions;
        int stride;
        boolean anyTable;
        String colOrIdx;

        Rule(String table, RuleType rule)
        {
            this.ruleType = rule;
            this.tableName = table;
            if ("*".equals(tableName)) {
                this.anyTable = true;
            }
        }

        public Rule withPartitions(int i)
        {
            this.partitions = i;
            return this;
        }

        public Rule withColOrIdx(String s)
        {
            this.colOrIdx = s;
            return this;
        }

        public Rule withStride(int i)
        {
            this.stride = i;
            return this;
        }

        public boolean isAnyTable()
        {
            return this.anyTable;
        }
    }

    public Rule newRule(String tableName, RuleType ruleName)
    {
        Rule r = new Rule(tableName, ruleName);
        rules.add(r);
        return r;
    }
}
