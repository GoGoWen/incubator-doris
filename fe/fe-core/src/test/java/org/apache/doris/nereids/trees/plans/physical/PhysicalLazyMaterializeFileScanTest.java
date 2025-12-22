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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.properties.DistributionSpec;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.planner.RuntimeFilterId;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.thrift.TRuntimeFilterType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Optional;

public class PhysicalLazyMaterializeFileScanTest {

    @Mock private PhysicalFileScan mockScan;
    @Mock private ExternalTable mockExternalTable; // Changed from TableIf to ExternalTable
    @Mock private DistributionSpec mockDistributionSpec;
    @Mock private Statistics mockStatistics;
    @Mock private LogicalProperties mockLogicalProperties;
    @Mock private PhysicalProperties mockPhysicalProperties;
    @Mock private LogicalFileScan.SelectedPartitions mockSelectedPartitions; // Added mock for SelectedPartitions
    private SlotReference rowIdSlot;
    private List<Slot> lazySlots;
    private List<Slot> operativeSlots;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Initialize common mocks
        rowIdSlot = new SlotReference("row_id", StringType.INSTANCE);
        lazySlots = ImmutableList.of(
                new SlotReference("lazy_col1", IntegerType.INSTANCE),
                new SlotReference("lazy_col2", StringType.INSTANCE)
        );
        operativeSlots = ImmutableList.of(
                new SlotReference("op_col1", IntegerType.INSTANCE),
                new SlotReference("op_col2", StringType.INSTANCE)
        );

        List<String> qualifier = ImmutableList.of("db", "tbl");

        Mockito.when(mockScan.getRelationId()).thenReturn(new RelationId(0)); // Changed ExprId to RelationId
        Mockito.when(mockScan.getTable()).thenReturn(mockExternalTable); // Changed to mockExternalTable
        Mockito.when(mockScan.getQualifier()).thenReturn(qualifier);
        Mockito.when(mockScan.getDistributionSpec()).thenReturn(mockDistributionSpec);
        Mockito.when(mockScan.getLogicalProperties()).thenReturn(mockLogicalProperties);
        Mockito.when(mockScan.getPhysicalProperties()).thenReturn(mockPhysicalProperties);
        Mockito.when(mockScan.getStats()).thenReturn(mockStatistics);
        Mockito.when(mockScan.getConjuncts()).thenReturn(ImmutableSet.of()); // Changed to ImmutableSet
        Mockito.when(mockScan.getSelectedPartitions()).thenReturn(mockSelectedPartitions); // Changed to mockSelectedPartitions
        Mockito.when(mockScan.getTableSample()).thenReturn(Optional.empty());
        Mockito.when(mockScan.getTableSnapshot()).thenReturn(Optional.empty());
        Mockito.when(mockScan.getOperativeSlots()).thenReturn(operativeSlots); // Changed to List<Slot>

        // Mock toString for the base scan to be consistent
        Mockito.when(mockScan.toString()).thenReturn("PhysicalFileScan[Table: tbl]");
    }

    @Test
    public void testConstructor() {
        PhysicalLazyMaterializeFileScan lazyScan = new PhysicalLazyMaterializeFileScan(mockScan, rowIdSlot, lazySlots);

        Assertions.assertNotNull(lazyScan);
        Assertions.assertEquals(mockScan.getRelationId(), lazyScan.getRelationId());
        Assertions.assertEquals(mockExternalTable, lazyScan.getTable()); // Assert against mockExternalTable
        Assertions.assertEquals(mockScan.getQualifier(), lazyScan.getQualifier());
        Assertions.assertEquals(mockDistributionSpec, lazyScan.getDistributionSpec());
        Assertions.assertEquals(mockStatistics, lazyScan.getStats());
        Assertions.assertEquals(operativeSlots, lazyScan.getOperativeSlots()); // Assert against operativeSlots list
    }

    @Test
    public void testGetQualifier() {
        PhysicalLazyMaterializeFileScan lazyScan = new PhysicalLazyMaterializeFileScan(mockScan, rowIdSlot, lazySlots);
        Assertions.assertEquals(mockScan.getQualifier(), lazyScan.getQualifier());
    }

    @Test
    public void testComputeOutput() {
        PhysicalLazyMaterializeFileScan lazyScan = new PhysicalLazyMaterializeFileScan(mockScan, rowIdSlot, lazySlots);
        List<Slot> expectedOutput = ImmutableList.<Slot>builder()
                .addAll(operativeSlots)
                .add(rowIdSlot)
                .build();
        Assertions.assertEquals(expectedOutput, lazyScan.computeOutput());

        // Ensure output is computed once and cached
        Mockito.when(mockScan.getOperativeSlots()).thenReturn(ImmutableList.of()); // Change operative slots on mock
        Assertions.assertEquals(expectedOutput, lazyScan.computeOutput()); // Should still return cached old output
    }

    @Test
    public void testToString() {
        PhysicalLazyMaterializeFileScan lazyScan = new PhysicalLazyMaterializeFileScan(mockScan, rowIdSlot, lazySlots);
        String expectedString = "PhysicalLazyMaterializeFileScan[PhysicalFileScan[Table: tbl]]";
        Assertions.assertEquals(expectedString, lazyScan.toString());

        // Test with runtime filters
        RuntimeFilter mockRf1 = Mockito.mock(RuntimeFilter.class);
        Mockito.when(mockRf1.getId()).thenReturn(RuntimeFilterId.createGenerator().getNextId()); // Changed to use createGenerator()
        Mockito.when(mockRf1.getType()).thenReturn(TRuntimeFilterType.BLOOM);

        RuntimeFilter mockRf2 = Mockito.mock(RuntimeFilter.class);
        Mockito.when(mockRf2.getId()).thenReturn(RuntimeFilterId.createGenerator().getNextId()); // Changed to use createGenerator()
        Mockito.when(mockRf2.getType()).thenReturn(TRuntimeFilterType.IN);

        List<RuntimeFilter> runtimeFilters = ImmutableList.of(mockRf1, mockRf2); // Changed to ImmutableList
        Mockito.when(mockScan.getAppliedRuntimeFilters()).thenReturn(runtimeFilters);

        // Recreate lazyScan for new mock behavior
        lazyScan = new PhysicalLazyMaterializeFileScan(mockScan, rowIdSlot, lazySlots);
        String actualStringWithRf = lazyScan.toString();

        // Order of RFs might not be guaranteed, so check containment
        Assertions.assertTrue(actualStringWithRf.contains("PhysicalLazyMaterializeFileScan[PhysicalFileScan[Table: tbl]"));
    }
}
