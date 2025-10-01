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

package org.apache.doris.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TreeNodeTest {

    /**
     * Test TreeNode class for testing foreachDown method
     */
    static class TestTreeNode extends TreeNode<TestTreeNode> {
        private final String name;

        public TestTreeNode(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "TestTreeNode(" + name + ")";
        }
    }

    @Test
    public void testForeachDown_FullTraversal() {
        // Build tree: root -> [child1 -> [grandchild1, grandchild2], child2]
        TestTreeNode root = new TestTreeNode("root");
        TestTreeNode child1 = new TestTreeNode("child1");
        TestTreeNode child2 = new TestTreeNode("child2");
        TestTreeNode grandchild1 = new TestTreeNode("grandchild1");
        TestTreeNode grandchild2 = new TestTreeNode("grandchild2");

        root.addChild(child1);
        root.addChild(child2);
        child1.addChild(grandchild1);
        child1.addChild(grandchild2);

        // Visit all nodes
        List<String> visited = new ArrayList<>();
        root.foreachDown(node -> {
            visited.add(((TestTreeNode) node).getName());
            return true; // Continue traversal
        });

        // Should visit all 5 nodes
        Assert.assertEquals(5, visited.size());
        Assert.assertEquals("root", visited.get(0));
        Assert.assertTrue(visited.contains("child1"));
        Assert.assertTrue(visited.contains("child2"));
        Assert.assertTrue(visited.contains("grandchild1"));
        Assert.assertTrue(visited.contains("grandchild2"));
    }

    @Test
    public void testForeachDown_EarlyTermination() {
        // Build simple tree
        TestTreeNode root = new TestTreeNode("root");
        TestTreeNode child1 = new TestTreeNode("child1");
        TestTreeNode child2 = new TestTreeNode("child2");
        root.addChild(child1);
        root.addChild(child2);

        // Stop at root - should not visit children
        AtomicInteger count = new AtomicInteger(0);
        root.foreachDown(node -> {
            count.incrementAndGet();
            return false; // Stop traversal
        });

        Assert.assertEquals(1, count.get()); // Only root visited
    }

    @Test
    public void testForeachDown_SelectiveTraversal() {
        // Build tree with multiple branches
        TestTreeNode root = new TestTreeNode("root");
        TestTreeNode child1 = new TestTreeNode("stop");
        TestTreeNode child2 = new TestTreeNode("continue");
        TestTreeNode grandchild1 = new TestTreeNode("skipped");
        TestTreeNode grandchild2 = new TestTreeNode("visited");

        root.addChild(child1);
        root.addChild(child2);
        child1.addChild(grandchild1);
        child2.addChild(grandchild2);

        // Stop at nodes named "stop"
        List<String> visited = new ArrayList<>();
        root.foreachDown(node -> {
            String name = ((TestTreeNode) node).getName();
            visited.add(name);
            return !name.equals("stop"); // Stop if name is "stop"
        });

        // Should visit: root, stop, continue, visited
        // Should NOT visit: skipped (child of "stop")
        Assert.assertEquals(4, visited.size());
        Assert.assertTrue(visited.contains("root"));
        Assert.assertTrue(visited.contains("stop"));
        Assert.assertTrue(visited.contains("continue"));
        Assert.assertTrue(visited.contains("visited"));
        Assert.assertFalse(visited.contains("skipped"));
    }

    @Test
    public void testForeachDown_EmptyChildren() {
        // Single node with no children
        TestTreeNode root = new TestTreeNode("lonely");

        AtomicInteger count = new AtomicInteger(0);
        root.foreachDown(node -> {
            count.incrementAndGet();
            return true;
        });

        Assert.assertEquals(1, count.get()); // Only root visited
    }

    @Test
    public void testForeachDown_DeepTree() {
        // Build deep tree (depth 4)
        TestTreeNode root = new TestTreeNode("level0");
        TestTreeNode level1 = new TestTreeNode("level1");
        TestTreeNode level2 = new TestTreeNode("level2");
        TestTreeNode level3 = new TestTreeNode("level3");

        root.addChild(level1);
        level1.addChild(level2);
        level2.addChild(level3);

        List<String> visited = new ArrayList<>();
        root.foreachDown(node -> {
            visited.add(((TestTreeNode) node).getName());
            return true;
        });

        // Should visit all 4 levels in order
        Assert.assertEquals(4, visited.size());
        Assert.assertEquals("level0", visited.get(0));
        Assert.assertEquals("level1", visited.get(1));
        Assert.assertEquals("level2", visited.get(2));
        Assert.assertEquals("level3", visited.get(3));
    }

    @Test
    public void testForeachDown_CountNodes() {
        // Build complex tree to verify visitor is called correct number of times
        TestTreeNode root = new TestTreeNode("root");
        TestTreeNode child1 = new TestTreeNode("child1");
        TestTreeNode child2 = new TestTreeNode("child2");
        TestTreeNode child3 = new TestTreeNode("child3");
        TestTreeNode grandchild1 = new TestTreeNode("grandchild1");
        TestTreeNode grandchild2 = new TestTreeNode("grandchild2");

        root.addChild(child1);
        root.addChild(child2);
        root.addChild(child3);
        child1.addChild(grandchild1);
        child2.addChild(grandchild2);

        // Count all nodes
        AtomicInteger count = new AtomicInteger(0);
        root.foreachDown(node -> {
            count.incrementAndGet();
            return true;
        });

        // Total: 1 root + 3 children + 2 grandchildren = 6 nodes
        Assert.assertEquals(6, count.get());

        // Verify matches numNodes() method
        Assert.assertEquals(root.numNodes(), count.get());
    }
}
