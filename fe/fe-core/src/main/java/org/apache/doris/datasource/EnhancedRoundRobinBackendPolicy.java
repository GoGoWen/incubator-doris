// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.system.BeSelectionPolicy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class EnhancedRoundRobinBackendPolicy {

    private static final Logger LOG = LogManager.getLogger(EnhancedRoundRobinBackendPolicy.class);
    List<Backend> backends = new ArrayList<>();

    public EnhancedRoundRobinBackendPolicy() {
        // scan node is used for query
        BeSelectionPolicy policy = new BeSelectionPolicy.Builder()
                .needQueryAvailable()
                .needLoadAvailable()
                .preferComputeNode(Config.prefer_compute_node_for_external_table)
                .assignExpectBeNum(Config.min_backend_num_for_external_table)
                .build();
        backends.addAll(policy.getCandidateBackends(Env.getCurrentSystemInfo().getIdToBackend().values()));
    }

    /**
     *  assign split to be with EnhancedRoundRobinBackendPolicy
     *
     */
    public Multimap<Backend, Split> computeScanRangeAssignment(List<Split> splits) {
        ListMultimap<Backend, Split> assignment = ArrayListMultimap.create();
        int remainingSplits = splits.size();
        int currentSplit = 0;
        while (remainingSplits > 0) {
            int adjustedSplits = adjustSplitNumPerRound(remainingSplits, Config.assign_split_num_per_round);
            if (adjustedSplits <= 0) {
                break;
            }
            assignSplitForPerRound(assignment, splits, currentSplit, adjustedSplits);
            currentSplit += adjustedSplits;
            remainingSplits -= adjustedSplits;
        }
        return assignment;
    }

    /**
     * assign split for per round
     */
    @VisibleForTesting
    private void assignSplitForPerRound(Multimap<Backend, Split> assignment, List<Split> splits,
            int startSplitNum, int splitsInRound) {
        int n = backends.size();
        int splitsPerBe = splitsInRound / n;
        for (int i = 0; i < n; i++) {
            Backend be = backends.get(i);
            for (int j = 0; j < splitsPerBe; j++) {
                assignment.get(be).add(splits.get(startSplitNum++));
            }
        }
    }

    /**
     * adjust split num for per round
     */
    @VisibleForTesting
    private int adjustSplitNumPerRound(int remainingSplits, int splitsPerRound) {
        int n = backends.size();
        if (n == 0) {
            return 0;
        }
        if (remainingSplits <= n) {
            return remainingSplits;
        }
        int candidate = Math.min(splitsPerRound, remainingSplits);
        return (candidate % n == 0) ? candidate : (candidate / n) * n;
    }
}
