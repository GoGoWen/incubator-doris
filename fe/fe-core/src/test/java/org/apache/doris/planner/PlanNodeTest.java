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

package org.apache.doris.planner;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Unit tests for PlanNode.collectInCurrentFragment() and PlanNode.foreachDownInCurrentFragment()
 * methods added to fix legacy planner issues with parallel_pipeline_task_num=1.
 */
public class PlanNodeTest {

    // Helper method for collecting nodes in current fragment
    private static void collectInFragmentHelper(PlanNode node, Predicate<PlanNode> predicate,
                                               List<PlanNode> result, int fragmentId) {
        if (node.getFragmentId().asInt() != fragmentId) {
            return;
        }
        if (predicate.test(node)) {
            result.add(node);
        }
        for (PlanNode child : node.getChildren()) {
            collectInFragmentHelper(child, predicate, result, fragmentId);
        }
    }

    // Helper method for iterating nodes in current fragment
    private static void foreachInFragmentHelper(PlanNode node, Consumer<PlanNode> visitor, int fragmentId) {
        if (node.getFragmentId().asInt() != fragmentId) {
            return;
        }
        visitor.accept(node);
        for (PlanNode child : node.getChildren()) {
            foreachInFragmentHelper(child, visitor, fragmentId);
        }
    }

    /**
     * Test collectInCurrentFragment with a single node (no children).
     * Should collect the node itself if predicate matches.
     */
    @Test
    public void testCollectInCurrentFragment_SingleNode(@Mocked PlanNode node,
                                                         @Mocked PlanFragment fragment) {
        PlanFragmentId fragmentId = new PlanFragmentId(1);

        new Expectations() {{
                fragment.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                node.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                node.getFragment();
                result = fragment;
                minTimes = 0;

                node.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                node.collectInCurrentFragment((Predicate<PlanNode>) any);
                result = new mockit.Delegate<List<? extends PlanNode>>() {
                    @SuppressWarnings("unused")
                    List<? extends PlanNode> delegate(Predicate<PlanNode> predicate) {
                        List<PlanNode> res = Lists.newArrayList();
                        collectInFragmentHelper(node, predicate, res, fragmentId.asInt());
                        return res;
                    }
                };
                minTimes = 0;
            }};

        // Test with predicate that doesn't match (ScanNode check on non-ScanNode)
        List<ScanNode> scanNodes = node.collectInCurrentFragment(n -> n instanceof ScanNode);
        Assert.assertEquals(0, scanNodes.size());

        // Test with predicate that matches all
        List<PlanNode> allNodes = node.collectInCurrentFragment(n -> true);
        Assert.assertEquals(1, allNodes.size());
    }

    /**
     * Test collectInCurrentFragment with a tree where all nodes are in the same fragment.
     * Should collect all matching nodes.
     */
    @Test
    public void testCollectInCurrentFragment_SameFragment(@Mocked PlanNode root,
                                                           @Mocked ScanNode child1,
                                                           @Mocked ScanNode child2,
                                                           @Mocked PlanNode child3,
                                                           @Mocked PlanFragment fragment) {
        PlanFragmentId fragmentId = new PlanFragmentId(1);

        new Expectations() {{
                fragment.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                root.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                child1.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                child2.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                child3.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                root.getFragment();
                result = fragment;
                minTimes = 0;

                root.getChildren();
                result = Lists.newArrayList(child1, child2, child3);
                minTimes = 0;

                child1.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                child2.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                child3.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                root.collectInCurrentFragment((Predicate<PlanNode>) any);
                result = new mockit.Delegate<List<? extends PlanNode>>() {
                    @SuppressWarnings("unused")
                    List<? extends PlanNode> delegate(Predicate<PlanNode> predicate) {
                        List<PlanNode> res = Lists.newArrayList();
                        collectInFragmentHelper(root, predicate, res, fragmentId.asInt());
                        return res;
                    }
                };
                minTimes = 0;
            }};

        // Collect all ScanNodes
        List<ScanNode> scanNodes = root.collectInCurrentFragment(n -> n instanceof ScanNode);
        Assert.assertEquals(2, scanNodes.size());

        // Collect all nodes
        List<PlanNode> allNodes = root.collectInCurrentFragment(n -> true);
        Assert.assertEquals(4, allNodes.size());
    }

    /**
     * Test collectInCurrentFragment with nodes in different fragments.
     * Should only collect nodes in the same fragment as the root.
     */
    @Test
    public void testCollectInCurrentFragment_DifferentFragments(@Mocked PlanNode root,
                                                                 @Mocked ScanNode child1,
                                                                 @Mocked ScanNode child2,
                                                                 @Mocked PlanNode grandchild,
                                                                 @Mocked PlanFragment fragment1,
                                                                 @Mocked PlanFragment fragment2) {
        PlanFragmentId fragmentId1 = new PlanFragmentId(1);
        PlanFragmentId fragmentId2 = new PlanFragmentId(2);

        new Expectations() {{
                fragment1.getFragmentId();
                result = fragmentId1;
                minTimes = 0;

                fragment2.getFragmentId();
                result = fragmentId2;
                minTimes = 0;

                root.getFragmentId();
                result = fragmentId1;
                minTimes = 0;

                child1.getFragmentId();
                result = fragmentId1;
                minTimes = 0;

                // child2 is in a different fragment
                child2.getFragmentId();
                result = fragmentId2;
                minTimes = 0;

                grandchild.getFragmentId();
                result = fragmentId2;
                minTimes = 0;

                root.getFragment();
                result = fragment1;
                minTimes = 0;

                root.getChildren();
                result = Lists.newArrayList(child1, child2);
                minTimes = 0;

                child1.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                child2.getChildren();
                result = Lists.newArrayList(grandchild);
                minTimes = 0;

                grandchild.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                root.collectInCurrentFragment((Predicate<PlanNode>) any);
                result = new mockit.Delegate<List<? extends PlanNode>>() {
                    @SuppressWarnings("unused")
                    List<? extends PlanNode> delegate(Predicate<PlanNode> predicate) {
                        List<PlanNode> res = Lists.newArrayList();
                        collectInFragmentHelper(root, predicate, res, fragmentId1.asInt());
                        return res;
                    }
                };
                minTimes = 0;
            }};

        // Collect all ScanNodes - should only get child1, not child2 (different fragment)
        List<ScanNode> scanNodes = root.collectInCurrentFragment(n -> n instanceof ScanNode);
        Assert.assertEquals(1, scanNodes.size());

        // Collect all nodes - should get root and child1, but not child2 or grandchild
        List<PlanNode> allNodes = root.collectInCurrentFragment(n -> true);
        Assert.assertEquals(2, allNodes.size());
    }

    /**
     * Test collectInCurrentFragment with predicate that matches no nodes.
     */
    @Test
    public void testCollectInCurrentFragment_NoMatches(@Mocked PlanNode root,
                                                        @Mocked PlanNode child1,
                                                        @Mocked PlanNode child2,
                                                        @Mocked PlanFragment fragment) {
        PlanFragmentId fragmentId = new PlanFragmentId(1);

        new Expectations() {{
                fragment.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                root.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                child1.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                child2.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                root.getFragment();
                result = fragment;
                minTimes = 0;

                root.getChildren();
                result = Lists.newArrayList(child1, child2);
                minTimes = 0;

                child1.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                child2.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                root.collectInCurrentFragment((Predicate<PlanNode>) any);
                result = new mockit.Delegate<List<? extends PlanNode>>() {
                    @SuppressWarnings("unused")
                    List<? extends PlanNode> delegate(Predicate<PlanNode> predicate) {
                        List<PlanNode> res = Lists.newArrayList();
                        collectInFragmentHelper(root, predicate, res, fragmentId.asInt());
                        return res;
                    }
                };
                minTimes = 0;
            }};

        // Predicate that never matches
        List<ScanNode> scanNodes = root.collectInCurrentFragment(n -> false);
        Assert.assertEquals(0, scanNodes.size());
    }

    /**
     * Test foreachDownInCurrentFragment with a single node.
     * Visitor should be called once.
     */
    @Test
    public void testForeachDownInCurrentFragment_SingleNode(@Mocked PlanNode node,
                                                             @Mocked PlanFragment fragment) {
        PlanFragmentId fragmentId = new PlanFragmentId(1);
        AtomicInteger visitCount = new AtomicInteger(0);

        new Expectations() {{
                fragment.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                node.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                node.getFragment();
                result = fragment;
                minTimes = 0;

                node.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                node.foreachDownInCurrentFragment((Consumer<PlanNode>) any);
                result = new mockit.Delegate<Void>() {
                    @SuppressWarnings("unused")
                    void delegate(Consumer<PlanNode> visitor) {
                        foreachInFragmentHelper(node, visitor, fragmentId.asInt());
                    }
                };
                minTimes = 0;
            }};

        node.foreachDownInCurrentFragment(n -> visitCount.incrementAndGet());
        Assert.assertEquals(1, visitCount.get());
    }

    /**
     * Test foreachDownInCurrentFragment with all nodes in same fragment.
     * Visitor should be called for all nodes.
     */
    @Test
    public void testForeachDownInCurrentFragment_SameFragment(@Mocked PlanNode root,
                                                               @Mocked PlanNode child1,
                                                               @Mocked PlanNode child2,
                                                               @Mocked PlanNode grandchild,
                                                               @Mocked PlanFragment fragment) {
        PlanFragmentId fragmentId = new PlanFragmentId(1);
        AtomicInteger visitCount = new AtomicInteger(0);

        new Expectations() {{
                fragment.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                root.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                child1.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                child2.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                grandchild.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                root.getFragment();
                result = fragment;
                minTimes = 0;

                root.getChildren();
                result = Lists.newArrayList(child1, child2);
                minTimes = 0;

                child1.getChildren();
                result = Lists.newArrayList(grandchild);
                minTimes = 0;

                child2.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                grandchild.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                root.foreachDownInCurrentFragment((Consumer<PlanNode>) any);
                result = new mockit.Delegate<Void>() {
                    @SuppressWarnings("unused")
                    void delegate(Consumer<PlanNode> visitor) {
                        foreachInFragmentHelper(root, visitor, fragmentId.asInt());
                    }
                };
                minTimes = 0;
            }};

        root.foreachDownInCurrentFragment(n -> visitCount.incrementAndGet());
        // Should visit root, child1, child2, grandchild = 4 nodes
        Assert.assertEquals(4, visitCount.get());
    }

    /**
     * Test foreachDownInCurrentFragment with nodes in different fragments.
     * Visitor should only be called for nodes in the same fragment.
     */
    @Test
    public void testForeachDownInCurrentFragment_DifferentFragments(@Mocked PlanNode root,
                                                                     @Mocked PlanNode child1,
                                                                     @Mocked PlanNode child2,
                                                                     @Mocked PlanNode grandchild1,
                                                                     @Mocked PlanNode grandchild2,
                                                                     @Mocked PlanFragment fragment1,
                                                                     @Mocked PlanFragment fragment2) {
        PlanFragmentId fragmentId1 = new PlanFragmentId(1);
        PlanFragmentId fragmentId2 = new PlanFragmentId(2);
        AtomicInteger visitCount = new AtomicInteger(0);
        List<PlanNode> visitedNodes = Lists.newArrayList();

        new Expectations() {{
                fragment1.getFragmentId();
                result = fragmentId1;
                minTimes = 0;

                fragment2.getFragmentId();
                result = fragmentId2;
                minTimes = 0;

                root.getFragmentId();
                result = fragmentId1;
                minTimes = 0;

                child1.getFragmentId();
                result = fragmentId1;
                minTimes = 0;

                // child2 and its descendants are in fragment 2
                child2.getFragmentId();
                result = fragmentId2;
                minTimes = 0;

                grandchild1.getFragmentId();
                result = fragmentId1;
                minTimes = 0;

                grandchild2.getFragmentId();
                result = fragmentId2;
                minTimes = 0;

                root.getFragment();
                result = fragment1;
                minTimes = 0;

                root.getChildren();
                result = Lists.newArrayList(child1, child2);
                minTimes = 0;

                child1.getChildren();
                result = Lists.newArrayList(grandchild1);
                minTimes = 0;

                child2.getChildren();
                result = Lists.newArrayList(grandchild2);
                minTimes = 0;

                grandchild1.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                grandchild2.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                root.foreachDownInCurrentFragment((Consumer<PlanNode>) any);
                result = new mockit.Delegate<Void>() {
                    @SuppressWarnings("unused")
                    void delegate(Consumer<PlanNode> visitor) {
                        foreachInFragmentHelper(root, visitor, fragmentId1.asInt());
                    }
                };
                minTimes = 0;
            }};

        root.foreachDownInCurrentFragment(n -> {
            visitCount.incrementAndGet();
            visitedNodes.add(n);
        });

        // Should visit root, child1, grandchild1 (all in fragment 1)
        // Should NOT visit child2 or grandchild2 (in fragment 2)
        Assert.assertEquals(3, visitCount.get());
        Assert.assertTrue(visitedNodes.contains(root));
        Assert.assertTrue(visitedNodes.contains(child1));
        Assert.assertTrue(visitedNodes.contains(grandchild1));
        Assert.assertFalse(visitedNodes.contains(child2));
        Assert.assertFalse(visitedNodes.contains(grandchild2));
    }

    /**
     * Test foreachDownInCurrentFragment with complex tree structure.
     * Multiple branches with mixed fragment boundaries.
     */
    @Test
    public void testForeachDownInCurrentFragment_ComplexTree(@Mocked PlanNode root,
                                                              @Mocked PlanNode leftChild,
                                                              @Mocked PlanNode rightChild,
                                                              @Mocked PlanNode leftGrandchild1,
                                                              @Mocked PlanNode leftGrandchild2,
                                                              @Mocked PlanNode rightGrandchild,
                                                              @Mocked PlanFragment fragment1,
                                                              @Mocked PlanFragment fragment2) {
        PlanFragmentId fragmentId1 = new PlanFragmentId(1);
        PlanFragmentId fragmentId2 = new PlanFragmentId(2);
        AtomicInteger visitCount = new AtomicInteger(0);

        new Expectations() {{
                fragment1.getFragmentId();
                result = fragmentId1;
                minTimes = 0;

                fragment2.getFragmentId();
                result = fragmentId2;
                minTimes = 0;

                // Tree structure:
                //        root (fragment 1)
                //       /    \
                //  leftChild  rightChild (fragment 2)
                //     / \           |
                // lgc1   lgc2    rgc (all fragment 2)
                //  (f1)   (f2)

                root.getFragmentId();
                result = fragmentId1;
                minTimes = 0;

                leftChild.getFragmentId();
                result = fragmentId1;
                minTimes = 0;

                rightChild.getFragmentId();
                result = fragmentId2;
                minTimes = 0;

                leftGrandchild1.getFragmentId();
                result = fragmentId1;
                minTimes = 0;

                leftGrandchild2.getFragmentId();
                result = fragmentId2;
                minTimes = 0;

                rightGrandchild.getFragmentId();
                result = fragmentId2;
                minTimes = 0;

                root.getFragment();
                result = fragment1;
                minTimes = 0;

                root.getChildren();
                result = Lists.newArrayList(leftChild, rightChild);
                minTimes = 0;

                leftChild.getChildren();
                result = Lists.newArrayList(leftGrandchild1, leftGrandchild2);
                minTimes = 0;

                rightChild.getChildren();
                result = Lists.newArrayList(rightGrandchild);
                minTimes = 0;

                leftGrandchild1.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                leftGrandchild2.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                rightGrandchild.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                root.foreachDownInCurrentFragment((Consumer<PlanNode>) any);
                result = new mockit.Delegate<Void>() {
                    @SuppressWarnings("unused")
                    void delegate(Consumer<PlanNode> visitor) {
                        foreachInFragmentHelper(root, visitor, fragmentId1.asInt());
                    }
                };
                minTimes = 0;
            }};

        root.foreachDownInCurrentFragment(n -> visitCount.incrementAndGet());

        // Should visit: root, leftChild, leftGrandchild1 (all in fragment 1)
        // Should NOT visit: rightChild, leftGrandchild2, rightGrandchild (in fragment 2)
        Assert.assertEquals(3, visitCount.get());
    }

    /**
     * Test that collectInCurrentFragment correctly filters ScanNode instances
     * as used in the actual DistributedPlanner fix.
     */
    @Test
    public void testCollectInCurrentFragment_ScanNodeFiltering(@Mocked PlanNode root,
                                                                @Mocked ScanNode scan1,
                                                                @Mocked ScanNode scan2,
                                                                @Mocked PlanNode nonScanNode,
                                                                @Mocked PlanFragment fragment) {
        PlanFragmentId fragmentId = new PlanFragmentId(1);

        new Expectations() {{
                fragment.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                root.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                scan1.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                scan2.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                nonScanNode.getFragmentId();
                result = fragmentId;
                minTimes = 0;

                root.getFragment();
                result = fragment;
                minTimes = 0;

                root.getChildren();
                result = Lists.newArrayList(scan1, nonScanNode, scan2);
                minTimes = 0;

                scan1.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                scan2.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                nonScanNode.getChildren();
                result = new ArrayList<PlanNode>();
                minTimes = 0;

                root.collectInCurrentFragment((Predicate<PlanNode>) any);
                result = new mockit.Delegate<List<? extends PlanNode>>() {
                    @SuppressWarnings("unused")
                    List<? extends PlanNode> delegate(Predicate<PlanNode> predicate) {
                        List<PlanNode> res = Lists.newArrayList();
                        collectInFragmentHelper(root, predicate, res, fragmentId.asInt());
                        return res;
                    }
                };
                minTimes = 0;
            }};

        // This mimics the usage in DistributedPlanner:
        // List<ScanNode> scanNodes = result.getPlanRoot().collectInCurrentFragment(p -> p instanceof ScanNode);
        List<ScanNode> scanNodes = root.collectInCurrentFragment(p -> p instanceof ScanNode);

        Assert.assertEquals(2, scanNodes.size());
        Assert.assertTrue(scanNodes.contains(scan1));
        Assert.assertTrue(scanNodes.contains(scan2));
    }
}
