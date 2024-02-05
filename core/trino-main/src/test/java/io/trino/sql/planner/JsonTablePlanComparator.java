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
package io.trino.sql.planner;

import io.trino.operator.table.json.JsonTableColumn;
import io.trino.operator.table.json.JsonTableOrdinalityColumn;
import io.trino.operator.table.json.JsonTablePlanCross;
import io.trino.operator.table.json.JsonTablePlanLeaf;
import io.trino.operator.table.json.JsonTablePlanNode;
import io.trino.operator.table.json.JsonTablePlanSingle;
import io.trino.operator.table.json.JsonTablePlanUnion;
import io.trino.operator.table.json.JsonTableQueryColumn;
import io.trino.operator.table.json.JsonTableValueColumn;

import java.util.Comparator;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class JsonTablePlanComparator
{
    private JsonTablePlanComparator() {}

    public static Comparator<JsonTablePlanNode> planComparator()
    {
        return (actual, expected) -> {
            requireNonNull(actual, "actual is null");
            requireNonNull(expected, "expected is null");
            return compare(actual, expected) ? 0 : -1;
        };
    }

    private static boolean compare(JsonTablePlanNode left, JsonTablePlanNode right)
    {
        if (left == right) {
            return true;
        }
        if (left.getClass() != right.getClass()) {
            return false;
        }
        if (left instanceof JsonTablePlanLeaf leftPlan) {
            JsonTablePlanLeaf rightPlan = (JsonTablePlanLeaf) right;
            return leftPlan.path().equals(rightPlan.path()) &&
                    compareColumns(leftPlan.columns(), rightPlan.columns());
        }
        if (left instanceof JsonTablePlanSingle leftPlan) {
            JsonTablePlanSingle rightPlan = (JsonTablePlanSingle) right;
            return leftPlan.path().equals(rightPlan.path()) &&
                    compareColumns(leftPlan.columns(), rightPlan.columns()) &&
                    leftPlan.outer() == rightPlan.outer() &&
                    compare(leftPlan.child(), rightPlan.child());
        }
        List<JsonTablePlanNode> leftSiblings;
        List<JsonTablePlanNode> rightSiblings;
        if (left instanceof JsonTablePlanCross leftPlan) {
            leftSiblings = leftPlan.siblings();
            rightSiblings = ((JsonTablePlanCross) right).siblings();
        }
        else {
            leftSiblings = ((JsonTablePlanUnion) left).siblings();
            rightSiblings = ((JsonTablePlanUnion) right).siblings();
        }
        if (leftSiblings.size() != rightSiblings.size()) {
            return false;
        }
        for (int i = 0; i < leftSiblings.size(); i++) {
            if (!compare(leftSiblings.get(i), rightSiblings.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean compareColumns(List<JsonTableColumn> leftColumns, List<JsonTableColumn> rightColumns)
    {
        if (leftColumns.size() != rightColumns.size()) {
            return false;
        }
        for (int i = 0; i < leftColumns.size(); i++) {
            if (!compareColumn(leftColumns.get(i), rightColumns.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean compareColumn(JsonTableColumn left, JsonTableColumn right)
    {
        if (left.getClass() != right.getClass()) {
            return false;
        }
        if (left instanceof JsonTableOrdinalityColumn leftColumn) {
            return leftColumn.outputIndex() == ((JsonTableOrdinalityColumn) right).outputIndex();
        }
        if (left instanceof JsonTableQueryColumn leftColumn) {
            JsonTableQueryColumn rightColumn = (JsonTableQueryColumn) right;
            return leftColumn.outputIndex() == rightColumn.outputIndex() &&
                    leftColumn.function().equals(rightColumn.function()) &&
                    leftColumn.path().equals(rightColumn.path()) &&
                    leftColumn.wrapperBehavior() == rightColumn.wrapperBehavior() &&
                    leftColumn.emptyBehavior() == rightColumn.emptyBehavior() &&
                    leftColumn.errorBehavior() == rightColumn.errorBehavior();
        }
        JsonTableValueColumn leftColumn = (JsonTableValueColumn) left;
        JsonTableValueColumn rightColumn = (JsonTableValueColumn) right;
        return leftColumn.outputIndex() == rightColumn.outputIndex() &&
                leftColumn.function().equals(rightColumn.function()) &&
                leftColumn.path().equals(rightColumn.path()) &&
                leftColumn.emptyBehavior() == rightColumn.emptyBehavior() &&
                leftColumn.emptyDefaultInput() == rightColumn.emptyDefaultInput() &&
                leftColumn.errorBehavior() == rightColumn.errorBehavior() &&
                leftColumn.errorDefaultInput() == rightColumn.errorDefaultInput();
    }
}
