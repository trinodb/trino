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
package io.trino.sql.planner.plan;

public enum RowsPerMatch
{
    ONE {
        @Override
        public boolean isOneRow()
        {
            return true;
        }

        @Override
        public boolean isEmptyMatches()
        {
            return true;
        }

        @Override
        public boolean isUnmatchedRows()
        {
            return false;
        }
    },

    // ALL_SHOW_EMPTY option applies to the MATCH_RECOGNIZE clause.
    // Output all rows of every match, including empty matches.
    // In the case of an empty match, output the starting row of the match attempt.
    // Do not produce output for the rows matched within exclusion `{- ... -}`.
    ALL_SHOW_EMPTY {
        @Override
        public boolean isOneRow()
        {
            return false;
        }

        @Override
        public boolean isEmptyMatches()
        {
            return true;
        }

        @Override
        public boolean isUnmatchedRows()
        {
            return false;
        }
    },

    // ALL_OMIT_EMPTY option applies to the MATCH_RECOGNIZE clause.
    // Output all rows of every non-empty match.
    // Do not produce output for the rows matched within exclusion `{- ... -}`
    ALL_OMIT_EMPTY {
        @Override
        public boolean isOneRow()
        {
            return false;
        }

        @Override
        public boolean isEmptyMatches()
        {
            return false;
        }

        @Override
        public boolean isUnmatchedRows()
        {
            return false;
        }
    },

    // ALL_WITH_UNMATCHED option applies to the MATCH_RECOGNIZE clause.
    // Output all rows of every match, including empty matches.
    // Produce an additional output row for every unmatched row.
    // Pattern exclusions are not allowed with this option.
    ALL_WITH_UNMATCHED {
        @Override
        public boolean isOneRow()
        {
            return false;
        }

        @Override
        public boolean isEmptyMatches()
        {
            return true;
        }

        @Override
        public boolean isUnmatchedRows()
        {
            return true;
        }
    },

    // WINDOW option applies to pattern recognition within window specification.
    // Output one row for every input row:
    // - if the row is skipped by some previous match, produce output as for unmatched row
    // - if match is found (either empty or non-empty), output a single-row summary
    // - if no match is found, produce output as for unmatched row
    WINDOW {
        @Override
        public boolean isOneRow()
        {
            return true;
        }

        @Override
        public boolean isEmptyMatches()
        {
            return true;
        }

        @Override
        public boolean isUnmatchedRows()
        {
            return true;
        }
    };

    public abstract boolean isOneRow();

    public abstract boolean isEmptyMatches();

    public abstract boolean isUnmatchedRows();
}
