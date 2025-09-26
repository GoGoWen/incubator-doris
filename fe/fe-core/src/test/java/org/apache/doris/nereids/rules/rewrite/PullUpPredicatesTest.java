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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PullUpPredicatesTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");

        createTable("create table test.student (\n"
                + "id int not null,\n"
                + "name varchar(128),\n"
                + "age int,sex int)\n"
                + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");

        createTable("create table test.score (\n"
                + "sid int not null, \n"
                + "cid int not null, \n"
                + "grade double)\n"
                + "distributed by hash(sid,cid) buckets 10\n"
                + "properties('replication_num' = '1');");

        createTable("create table test.course (\n"
                + "id int not null, \n"
                + "name varchar(128), \n"
                + "teacher varchar(128))\n"
                + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");

        createTables("create table test.subquery1\n"
                        + "(k1 bigint, k2 bigint)\n"
                        + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');\n",
                "create table test.subquery2\n"
                        + "(k1 varchar(10), k2 bigint)\n"
                        + "partition by range(k2)\n"
                        + "(partition p1 values less than(\"10\"))\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');",
                "create table test.subquery3\n"
                        + "(k1 int not null, k2 varchar(128), k3 bigint, v1 bigint, v2 bigint)\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');",
                "create table test.subquery4\n"
                        + "(k1 bigint, k2 bigint)\n"
                        + "duplicate key(k1)\n"
                        + "distributed by hash(k2) buckets 1\n"
                        + "properties('replication_num' = '1');");

        createTables("CREATE TABLE `test`.`test_union` (\n"
                + "`key` varchar(*) NOT NULL,\n"
                + "  `value` varchar(*) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DISTRIBUTED BY HASH(`key`) BUCKETS 1\n"
                + "PROPERTIES ('replication_allocation' = 'tag.location.default: 1');");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().setEnableFoldConstantByBe(true);
    }

    @Test
    void testPullUpPredicatesFromLogicalOneRowRelation() {
        String sql = "select * from (select 1 as id, 'test' as name) t inner join score on t.id = score.sid";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalJoin(
                        any(),
                        logicalFilter(logicalOlapScan())
                        .when(filter -> filter.getPredicate().toSql().contains("sid = 1"))
                    )
                )
        );
    }

    @Test
    void testPullUpPredicatesFromLogicalFilter() {
        String sql = "select * from (select * from student where id > 10) t inner join score on t.id = score.sid";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalJoin(
                        logicalFilter(logicalOlapScan()),
                        logicalFilter(logicalOlapScan())
                        .when(filter -> filter.getPredicate().toSql().contains("sid > 10"))
                    )
                )
        );
    }

    @Test
    void testPullUpPredicatesFromLogicalProject() {
        String sql = "select t.new_id from (select id as new_id, name from student where id > 5) t inner join score on t.new_id = score.sid";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalProject(
                        logicalJoin(
                            logicalProject(
                                logicalFilter(logicalOlapScan())
                                .when(filter -> filter.getPredicate().toSql().contains("id > 5"))
                            ),
                            logicalProject(
                                logicalFilter(logicalOlapScan())
                                .when(filter -> filter.getPredicate().toSql().contains("sid > 5"))
                            )
                        )
                    )
                )
        );
    }

    @Test
    void testPullUpPredicatesFromLogicalAggregate() {
        String sql = "select t.sid, t.avg_grade from (select sid, avg(grade) as avg_grade from score where sid > 100 group by sid) t inner join student on t.sid = student.id";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalJoin(
                        logicalAggregate(
                            logicalProject(
                                logicalFilter(logicalOlapScan())
                                .when(filter -> filter.getPredicate().toSql().contains("sid > 100"))
                            )
                        ),
                        logicalProject(
                            logicalFilter(logicalOlapScan())
                            .when(filter -> filter.getPredicate().toSql().contains("id > 100"))
                        )
                    )
            )
        );
    }

    @Test
    void testPullUpPredicatesFromLogicalUnionWithConstantExpressions() {
        String sql = "select c1 from (select 1 c1 union all select 2 c1) t inner join score on t.c1 = score.sid";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalProject(
                        logicalJoin(
                            any(),
                            logicalProject(
                                logicalFilter(logicalOlapScan())
                                .when(filter -> filter.getPredicate().toSql().contains("sid IN (1, 2)"))
                            )
                        )
                    )
            )
        );
    }

    @Test
    void testPullUpPredicatesFromLogicalUnionWithSingleConstant() {
        String sql = "select c1 from (select 5 c1 union all select 5 c1) t inner join score on t.c1 = score.sid";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalProject(
                        logicalJoin(
                            any(),
                            logicalProject(
                                logicalFilter(logicalOlapScan())
                                .when(filter -> filter.getPredicate().toSql().contains("sid = 5"))
                            )
                        )
                    )
                )
        );
    }

    @Test
    void testPullUpPredicatesFromLogicalUnionWithChildren() {
        String sql = "select c1 from (select id c1 from student where id < 10 union all select sid c1 from score where sid < 10) t inner join course on t.c1 = course.id";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalProject(
                        logicalJoin(
                            any(),
                            logicalProject(
                                logicalFilter(logicalOlapScan())
                                .when(filter -> filter.getPredicate().toSql().contains("id < 10"))
                            )
                        )
                    )
                )
        );
    }

    @Test
    void testPullUpPredicatesFromLogicalIntersect() {
        String sql = "select c1 from (select id c1 from student where id < 20 intersect select sid c1 from score where sid > 5) t inner join course on t.c1 = course.id";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalProject(
                        logicalJoin(
                            any(),
                            logicalProject(
                                logicalFilter(logicalOlapScan())
                                .when(filter -> filter.getPredicate().toSql().contains("id < 20")
                                       && filter.getPredicate().toSql().contains("id > 5"))
                            )
                        )
                    )
                )
        );
    }

    @Test
    void testPullUpPredicatesFromLogicalExcept() {
        String sql = "select c1 from (select id c1 from student where id < 15 except select sid c1 from score where sid > 10) t inner join course on t.c1 = course.id";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalProject(
                        logicalJoin(
                            any(),
                            logicalProject(
                                logicalFilter(logicalOlapScan())
                                .when(filter -> filter.getPredicate().toSql().contains("id < 15"))
                            )
                        )
                    )
                )
        );
    }

    @Test
    void testPullUpPredicatesFromInnerJoin() {
        String sql = "select * from student inner join score on student.id = score.sid where student.age > 18";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalJoin(
                        logicalFilter(logicalOlapScan())
                        .when(filter -> filter.getPredicate().toSql().contains("age > 18")),
                        any()
                    )
                )
        );
    }

    @Test
    void testPullUpPredicatesFromCrossJoin() {
        String sql = "select * from student, score where student.id = score.sid and student.id > 50";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalJoin(
                        logicalFilter(logicalOlapScan())
                        .when(filter -> filter.getPredicate().toSql().contains("id > 50")),
                        logicalFilter(logicalOlapScan())
                        .when(filter -> filter.getPredicate().toSql().contains("sid > 50"))
                    )
                )
        );
    }

    @Test
    void testNoPullUpForLeftJoinConditions() {
        // Left join should not pull up predicates from join conditions to left side
        String sql = "select * from student left join score on student.id = score.sid and score.grade > 80";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalJoin(
                        logicalOlapScan(),
                        any()
                    )
                )
        );
    }

    @Test
    void testPullUpPredicatesWithMultipleJoins() {
        String sql = "select * from student inner join score on student.id = score.sid inner join course on score.cid = course.id where student.age > 20";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalJoin(
                        logicalJoin(
                            logicalFilter(logicalOlapScan())
                            .when(filter -> filter.getPredicate().toSql().contains("age > 20")),
                            any()
                        ),
                        any()
                    )
                )
        );
    }

    @Test
    void testPullUpPredicatesWithComplexExpression() {
        String sql = "select * from (select id, name, age + 1 as new_age from student where id in (1, 2, 3)) t inner join score on t.id = score.sid";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalJoin(
                        logicalProject(
                            logicalFilter(logicalOlapScan())
                            .when(filter -> filter.getPredicate().toSql().contains("id IN (1, 2, 3)"))
                        ),
                        logicalFilter(logicalOlapScan())
                        .when(filter -> filter.getPredicate().toSql().contains("sid IN (1, 2, 3)"))
                    )
                )
        );
    }

    @Test
    void testPullUpPredicatesFromNestedSubqueries() {
        String sql = "select * from (select * from (select * from student where age > 25) s where s.id > 100) t inner join score on t.id = score.sid";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalJoin(
                        logicalFilter(logicalOlapScan())
                        .when(filter -> filter.getPredicate().toSql().contains("age > 25")
                               && filter.getPredicate().toSql().contains("id > 100")),
                        logicalFilter(logicalOlapScan())
                        .when(filter -> filter.getPredicate().toSql().contains("sid > 100"))
                    )
                )
        );
    }

    @Test
    void testPullUpPredicatesWithMixedUnionAndConstants() {
        // This test checks mixed union with constants and children
        // The union has: 10 from course (where id < 50) and age from student (where age > 0)
        // Since the first branch has constant 10 and second has age, no common predicate can be pulled up
        String sql = "select c1 from (select 10 c1, id from course where id < 50 union all select age, id from student where age > 0) t inner join score on t.id = score.sid and t.c1 = score.cid";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalProject(
                        logicalJoin(
                            logicalUnion(
                                logicalProject(
                                    logicalFilter(logicalOlapScan())
                                    .when(filter -> filter.getPredicate().toSql().contains("id < 50"))
                                ),
                                logicalProject(
                                    logicalFilter(logicalOlapScan())
                                    .when(filter -> filter.getPredicate().toSql().contains("age > 0"))
                                )
                            ),
                            any() // No filter pushed down to score because no common predicate from union
                        )
                    )
                )
        );
    }

    @Test
    void testCachingBehavior() {
        // Test that multiple calls with same plan structure use caching
        String sql1 = "select * from (select * from student where id > 10) t1 inner join score on t1.id = score.sid";
        String sql2 = "select * from (select * from student where id > 10) t2 inner join score on t2.id = score.sid";

        Assertions.assertDoesNotThrow(
                () -> {
                    PlanChecker.from(connectContext).analyze(sql1).rewrite();
                    PlanChecker.from(connectContext).analyze(sql2).rewrite();
                }
        );
    }

    @Test
    void testEdgeCaseEmptyPredicates() {
        // Test with empty predicates - should not fail
        String sql = "select * from student inner join score on student.id = score.sid";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalJoin(
                        logicalOlapScan(),
                        logicalOlapScan()
                    )
                )
        );
    }

    @Test
    void testEdgeCaseNullLiteralsInUnion() {
        // Test union with null literals - should filter them out
        String sql = "select c1 from (select 1 c1 union all select null c1) t inner join score on t.c1 = score.sid";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalProject(
                        logicalJoin(
                            any(),
                            logicalProject(
                                logicalFilter(logicalOlapScan())
                                .when(filter -> filter.getPredicate().toSql().contains("sid = 1"))
                            )
                        )
                    )
                )
        );
    }

    @Test
    void testComplexPredicateWithMultipleColumns() {
        String sql = "select * from (select id, age from student where id > 5 and age < 30) t inner join score on t.id = score.sid";

        Assertions.assertDoesNotThrow(
                () -> PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                    logicalJoin(
                        logicalProject(
                            logicalFilter(logicalOlapScan())
                            .when(filter -> filter.getPredicate().toSql().contains("id > 5")
                                   && filter.getPredicate().toSql().contains("age < 30"))
                        ),
                        logicalFilter(logicalOlapScan())
                        .when(filter -> filter.getPredicate().toSql().contains("sid > 5"))
                    )
                )
        );
    }
}
