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
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class EnhancedRoundRobinBackendPolicyTest {

    private int originalAssignSplitNumPerRound;

    @Before
    public void setUp() {
        originalAssignSplitNumPerRound = Config.assign_split_num_per_round;
    }

    @After
    public void tearDown() {
        Config.assign_split_num_per_round = originalAssignSplitNumPerRound;
    }

    /**
     * Test basic split assignment with multiple backends
     */
    @Test
    public void testComputeScanRangeAssignment(@Mocked Env env,
            @Mocked SystemInfoService systemInfoService) throws Exception {
        // Setup mock backends
        List<Backend> mockBackends = createMockBackends(3);
        ImmutableMap.Builder<Long, Backend> builder = ImmutableMap.builder();
        for (Backend backend : mockBackends) {
            builder.put(backend.getId(), backend);
        }
        ImmutableMap<Long, Backend> idToBackend = builder.build();

        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }
        };

        new Expectations() {
            {
                systemInfoService.getIdToBackend();
                result = idToBackend;
            }
        };

        // Create test splits
        List<Split> splits = createMockSplits(9); // 9 splits for 3 backends
        Config.assign_split_num_per_round = 6; // 2 splits per backend per round

        EnhancedRoundRobinBackendPolicy policy = new EnhancedRoundRobinBackendPolicy();
        Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);

        // Verify assignment
        Assertions.assertEquals(3, assignment.keySet().size());

        // Each backend should get 3 splits (9 total / 3 backends)
        for (Backend backend : assignment.keySet()) {
            Collection<Split> assignedSplits = assignment.get(backend);
            Assertions.assertEquals(3, assignedSplits.size());
        }
    }

    /**
     * Test split assignment when splits are not evenly divisible by backends
     */
    @Test
    public void testComputeScanRangeAssignmentUnevenSplits(@Mocked Env env,
            @Mocked SystemInfoService systemInfoService)
            throws Exception {
        // Setup mock backends
        List<Backend> mockBackends = createMockBackends(3);
        ImmutableMap.Builder<Long, Backend> builder = ImmutableMap.builder();
        for (Backend backend : mockBackends) {
            builder.put(backend.getId(), backend);
        }
        ImmutableMap<Long, Backend> idToBackend = builder.build();

        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }
        };

        new Expectations() {
            {
                systemInfoService.getIdToBackend();
                result = idToBackend;
            }
        };

        // Create test splits - 10 splits for 3 backends
        List<Split> splits = createMockSplits(10);
        Config.assign_split_num_per_round = 6; // 2 splits per backend per round

        EnhancedRoundRobinBackendPolicy policy = new EnhancedRoundRobinBackendPolicy();
        Multimap<Backend, Split> assignment = policy.computeScanRangeAssignment(splits);

        // Verify assignment
        Assertions.assertEquals(3, assignment.keySet().size());

        // Total splits should be assigned (some backends may get more than others)
        int totalAssigned = 0;
        for (Backend backend : assignment.keySet()) {
            totalAssigned += assignment.get(backend).size();
        }
        Assertions.assertEquals(9, totalAssigned); // Only 9 splits assigned (3*3, rounded down)
    }

    /**
     * Test adjustSplitNumPerRound method
     */
    @Test
    public void testAdjustSplitNumPerRound(@Mocked Env env,
            @Mocked SystemInfoService systemInfoService) throws Exception {
        // Setup mock backends
        List<Backend> mockBackends = createMockBackends(3);
        ImmutableMap.Builder<Long, Backend> builder = ImmutableMap.builder();
        for (Backend backend : mockBackends) {
            builder.put(backend.getId(), backend);
        }
        ImmutableMap<Long, Backend> idToBackend = builder.build();

        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }
        };

        new Expectations() {
            {
                systemInfoService.getIdToBackend();
                result = idToBackend;
            }
        };

        EnhancedRoundRobinBackendPolicy policy = new EnhancedRoundRobinBackendPolicy();

        // Use reflection to test private method
        Method adjustMethod = EnhancedRoundRobinBackendPolicy.class.getDeclaredMethod(
                "adjustSplitNumPerRound", int.class, int.class);
        adjustMethod.setAccessible(true);

        // Test cases
        // Case 1: Normal case - 10 remaining splits, 6 splits per round, 3 backends
        int result1 = (Integer) adjustMethod.invoke(policy, 10, 6);
        Assertions.assertEquals(6, result1); // 6 is divisible by 3

        // Case 2: Remaining splits less than backends
        int result2 = (Integer) adjustMethod.invoke(policy, 2, 6);
        Assertions.assertEquals(2, result2); // Return remaining splits

        // Case 3: Splits per round not divisible by backends
        int result3 = (Integer) adjustMethod.invoke(policy, 10, 7);
        Assertions.assertEquals(6, result3); // 7/3 = 2, so 2*3 = 6

        // Case 4: Remaining splits less than splits per round
        int result4 = (Integer) adjustMethod.invoke(policy, 5, 9);
        Assertions.assertEquals(3, result4); // 5/3 = 1, so 1*3 = 3
    }

    /**
     * Test assignSplitForPerRound method
     */
    @Test
    public void testAssignSplitForPerRound(@Mocked Env env,
            @Mocked SystemInfoService systemInfoService) throws Exception {
        // Setup mock backends
        List<Backend> mockBackends = createMockBackends(2);
        ImmutableMap.Builder<Long, Backend> builder = ImmutableMap.builder();
        for (Backend backend : mockBackends) {
            builder.put(backend.getId(), backend);
        }
        ImmutableMap<Long, Backend> idToBackend = builder.build();

        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }
        };

        new Expectations() {
            {
                systemInfoService.getIdToBackend();
                result = idToBackend;
            }
        };

        EnhancedRoundRobinBackendPolicy policy = new EnhancedRoundRobinBackendPolicy();

        // Create test splits
        List<Split> splits = createMockSplits(6);

        // Use reflection to test private method
        Method assignMethod = EnhancedRoundRobinBackendPolicy.class.getDeclaredMethod(
                "assignSplitForPerRound", Multimap.class, List.class, int.class, int.class);
        assignMethod.setAccessible(true);

        com.google.common.collect.ListMultimap<Backend, Split> assignment =
                com.google.common.collect.ArrayListMultimap.create();

        // Assign 4 splits starting from index 0 (2 splits per backend)
        assignMethod.invoke(policy, assignment, splits, 0, 4);

        // Verify assignment
        Assertions.assertEquals(2, assignment.keySet().size());
        for (Backend backend : assignment.keySet()) {
            Assertions.assertEquals(2, assignment.get(backend).size());
        }
    }

    /**
     * Test with no backends available
     */
    @Test
    public void testNoBackendsAvailable(@Mocked Env env,
            @Mocked SystemInfoService systemInfoService) throws Exception {
        ImmutableMap<Long, Backend> emptyBackends = ImmutableMap.of();

        new MockUp<Env>() {
            @Mock
            public SystemInfoService getCurrentSystemInfo() {
                return systemInfoService;
            }
        };

        new Expectations() {
            {
                systemInfoService.getIdToBackend();
                result = emptyBackends;
            }
        };

        EnhancedRoundRobinBackendPolicy policy = new EnhancedRoundRobinBackendPolicy();

        // Use reflection to test adjustSplitNumPerRound with no backends
        Method adjustMethod = EnhancedRoundRobinBackendPolicy.class.getDeclaredMethod(
                "adjustSplitNumPerRound", int.class, int.class);
        adjustMethod.setAccessible(true);

        int result = (Integer) adjustMethod.invoke(policy, 10, 6);
        Assertions.assertEquals(0, result); // Should return 0 when no backends
    }

    private List<Backend> createMockBackends(int count) {
        List<Backend> backends = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Backend backend = new Backend(i + 1, "host" + (i + 1), 9030);
            backend.setAlive(true);
            backends.add(backend);
        }
        return backends;
    }

    private List<Split> createMockSplits(int count) {
        List<Split> splits = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            splits.add(new MockSplit("split" + i));
        }
        return splits;
    }

    // Mock Split implementation for testing
    private static class MockSplit implements Split {
        private final String name;
        private List<String> alternativeHosts = new ArrayList<>();

        public MockSplit(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public String[] getHosts() {
            return new String[0];
        }

        @Override
        public Object getInfo() {
            return null;
        }

        @Override
        public boolean isRemotelyAccessible() {
            return true;
        }

        @Override
        public String getPathString() {
            return name;
        }

        @Override
        public long getStart() {
            return 0;
        }

        @Override
        public long getLength() {
            return 100;
        }

        @Override
        public List<String> getAlternativeHosts() {
            return alternativeHosts;
        }

        @Override
        public void setAlternativeHosts(List<String> alternativeHosts) {
            this.alternativeHosts = alternativeHosts;
        }
    }
}
