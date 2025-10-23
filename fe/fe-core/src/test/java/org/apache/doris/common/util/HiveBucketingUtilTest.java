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

package org.apache.doris.common.util;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.hive.AcidInfo;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;

import com.google.common.collect.Lists;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;


public class HiveBucketingUtilTest {
    @Test
    public void testBucketingVersionV1()  {
        Assert.assertEquals(1, HiveBucketingUtil.BucketingVersion.BUCKETING_V1.getVersion());
        Assert.assertEquals(HiveBucketingUtil.BucketingVersion.BUCKETING_V1, HiveBucketingUtil.getBucketingVersion(1));

        HiveBucketingUtil.BucketingVersion version = HiveBucketingUtil.BucketingVersion.BUCKETING_V1;

        Assert.assertEquals(1, version.hash(Type.BOOLEAN, true));
        Assert.assertEquals(0, version.hash(Type.BOOLEAN, false));
        Assert.assertEquals(0, version.hash(Type.BOOLEAN, null));


        Assert.assertEquals(100, version.hash(Type.TINYINT, 100L));
        Assert.assertEquals(-50, version.hash(Type.TINYINT, -50L));
        Assert.assertEquals(127, version.hash(Type.TINYINT, 127L));
        Assert.assertEquals(-128, version.hash(Type.TINYINT, -128L));
        Assert.assertEquals(0, version.hash(Type.TINYINT, null));


        Assert.assertEquals(1000, version.hash(Type.SMALLINT, 1000L));
        Assert.assertEquals(-2000, version.hash(Type.SMALLINT, -2000L));
        Assert.assertEquals(32767, version.hash(Type.SMALLINT, 32767L));
        Assert.assertEquals(-32768, version.hash(Type.SMALLINT, -32768L));
        Assert.assertEquals(0, version.hash(Type.SMALLINT, null));


        Assert.assertEquals(100000, version.hash(Type.INT, 100000L));
        Assert.assertEquals(-200000, version.hash(Type.INT, -200000L));
        Assert.assertEquals(Integer.MAX_VALUE, version.hash(Type.INT, (long) Integer.MAX_VALUE));
        Assert.assertEquals(Integer.MIN_VALUE, version.hash(Type.INT, (long) Integer.MIN_VALUE));
        Assert.assertEquals(0, version.hash(Type.INT, null));

        long value1 = 123456789012345L;
        int expected1 = (int) ((value1 >>> 32) ^ value1);
        Assert.assertEquals(expected1, version.hash(Type.BIGINT, value1));
        long value2 = -98765432109876L;
        int expected2 = (int) ((value2 >>> 32) ^ value2);
        Assert.assertEquals(expected2, version.hash(Type.BIGINT, value2));
        long max = Long.MAX_VALUE;
        int expectedMax = (int) ((max >>> 32) ^ max);
        Assert.assertEquals(expectedMax, version.hash(Type.BIGINT, max));
        long min = Long.MIN_VALUE;
        int expectedMin = (int) ((min >>> 32) ^ min);
        Assert.assertEquals(expectedMin, version.hash(Type.BIGINT, min));
        Assert.assertEquals(0, version.hash(Type.BIGINT, null));

        Type type = Type.STRING;
        String eng = "hello";
        int engExpected = 99162322;
        Assert.assertEquals(engExpected, version.hash(type, eng));
        String chinese = "中文测试";
        int chineseExpected = -793140368;
        Assert.assertEquals(chineseExpected, version.hash(type, chinese));
        Assert.assertEquals(0, version.hash(type, ""));
        Assert.assertEquals(0, version.hash(type, null));

        try {
            version.hash(Type.DATE, "2025-10-19");
            Assert.fail("hash date type should throw unsupported exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UnsupportedOperationException);
            Assert.assertEquals("Computation of Hive bucket hashCode is not supported for Hive primitive"
                    + " category: date", e.getMessage());
        }
    }

    @Test
    public void testBucketingVersionV2()  {
        Assert.assertEquals(2, HiveBucketingUtil.BucketingVersion.BUCKETING_V2.getVersion());
        Assert.assertEquals(HiveBucketingUtil.BucketingVersion.BUCKETING_V2, HiveBucketingUtil.getBucketingVersion(2));

        HiveBucketingUtil.BucketingVersion version = HiveBucketingUtil.BucketingVersion.BUCKETING_V2;

        Assert.assertEquals(1, version.hash(Type.BOOLEAN, true));
        Assert.assertEquals(0, version.hash(Type.BOOLEAN, false));
        Assert.assertEquals(0, version.hash(Type.BOOLEAN, null));


        Assert.assertEquals(100, version.hash(Type.TINYINT, 100L));
        Assert.assertEquals(-50, version.hash(Type.TINYINT, -50L));
        Assert.assertEquals(127, version.hash(Type.TINYINT, 127L));
        Assert.assertEquals(-128, version.hash(Type.TINYINT, -128L));
        Assert.assertEquals(0, version.hash(Type.TINYINT, null));


        Assert.assertEquals(-820329743, version.hash(Type.SMALLINT, 1000L));
        Assert.assertEquals(60503773, version.hash(Type.SMALLINT, -2000L));
        Assert.assertEquals(-684075052, version.hash(Type.SMALLINT, 32767L));
        Assert.assertEquals(1342976838, version.hash(Type.SMALLINT, -32768L));
        Assert.assertEquals(0, version.hash(Type.SMALLINT, null));


        Assert.assertEquals(-135065790, version.hash(Type.INT, 100000L));
        Assert.assertEquals(32872620, version.hash(Type.INT, -200000L));
        Assert.assertEquals(1133859967, version.hash(Type.INT, (long) Integer.MAX_VALUE));
        Assert.assertEquals(1194881028, version.hash(Type.INT, (long) Integer.MIN_VALUE));
        Assert.assertEquals(0, version.hash(Type.INT, null));

        long value1 = 123456789012345L;
        Assert.assertEquals(-304250964, version.hash(Type.BIGINT, value1));
        long value2 = -98765432109876L;
        Assert.assertEquals(-477519799, version.hash(Type.BIGINT, value2));
        long max = Long.MAX_VALUE;
        Assert.assertEquals(-536577852, version.hash(Type.BIGINT, max));
        long min = Long.MIN_VALUE;
        Assert.assertEquals(1728983947, version.hash(Type.BIGINT, min));
        Assert.assertEquals(0, version.hash(Type.BIGINT, null));

        Type type = Type.STRING;
        String eng = "hello";
        Assert.assertEquals(1321743225, version.hash(type, eng));
        String chinese = "中文测试";
        Assert.assertEquals(-1305815978, version.hash(type, chinese));
        Assert.assertEquals(-965378730, version.hash(type, ""));
        Assert.assertEquals(0, version.hash(type, null));
        try {
            version.hash(Type.DATE, "2025-10-19");
            Assert.fail("hash date type should throw unsupported exception");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof UnsupportedOperationException);
            Assert.assertEquals("Computation of Hive bucket hashCode is not supported for Hive primitive"
                    + " category: date", e.getMessage());
        }
    }

    @Test
    public void testHiveBucketingProperty() {
        HiveBucketingUtil.HiveBucketProperty hiveBucketProperty1 =
                new HiveBucketingUtil.HiveBucketProperty(Lists.newArrayList("test1"), 500,
                HiveBucketingUtil.BucketingVersion.BUCKETING_V1);
        Assert.assertEquals(1, hiveBucketProperty1.getBucketedBy().size());
        Assert.assertEquals("test1", hiveBucketProperty1.getBucketedBy().get(0));
        Assert.assertEquals(500, hiveBucketProperty1.getBucketCount());
        Assert.assertEquals(HiveBucketingUtil.BucketingVersion.BUCKETING_V1,
                hiveBucketProperty1.getBucketingVersion());
        HiveBucketingUtil.HiveBucketProperty hiveBucketProperty2 =
                new HiveBucketingUtil.HiveBucketProperty(Lists.newArrayList("test2"), 1000,
                HiveBucketingUtil.BucketingVersion.BUCKETING_V2);
        Assert.assertEquals(1, hiveBucketProperty2.getBucketedBy().size());
        Assert.assertEquals("test2", hiveBucketProperty2.getBucketedBy().get(0));
        Assert.assertEquals(1000, hiveBucketProperty2.getBucketCount());
        Assert.assertEquals(HiveBucketingUtil.BucketingVersion.BUCKETING_V2,
                hiveBucketProperty2.getBucketingVersion());
    }

    @Test
    public void testGetBucketNumber() {
        Assert.assertEquals(OptionalInt.of(1), HiveBucketingUtil.getBucketNumber("000001_0"));
        Assert.assertEquals(OptionalInt.of(123), HiveBucketingUtil.getBucketNumber("000123_0"));
        Assert.assertEquals(OptionalInt.of(999), HiveBucketingUtil.getBucketNumber("000999_0"));

        Assert.assertEquals(OptionalInt.of(42), HiveBucketingUtil.getBucketNumber("000042_0.parquet"));
        Assert.assertEquals(OptionalInt.of(100), HiveBucketingUtil.getBucketNumber("000100_0_snappy.orc"));
        Assert.assertEquals(OptionalInt.of(255), HiveBucketingUtil.getBucketNumber("000255_0.gz"));

        Assert.assertEquals(OptionalInt.of(0), HiveBucketingUtil.getBucketNumber("000000_0"));
        Assert.assertEquals(OptionalInt.of(1000), HiveBucketingUtil.getBucketNumber("001000_0"));

        Assert.assertEquals(OptionalInt.of(1),
                HiveBucketingUtil.getBucketNumber("20230101_120000_00001_abcde_bucket-001"));
        Assert.assertEquals(OptionalInt.of(42),
                HiveBucketingUtil.getBucketNumber("20230215_084523_12345_f1a2b_bucket-042"));
        Assert.assertEquals(OptionalInt.of(255),
                HiveBucketingUtil.getBucketNumber("20231231_235959_99999_zzzzz_bucket-255"));
        Assert.assertEquals(OptionalInt.of(100),
                HiveBucketingUtil.getBucketNumber("20230101_120000_00001_abcde_bucket-100.parquet"));
        Assert.assertEquals(OptionalInt.of(200),
                HiveBucketingUtil.getBucketNumber("20230101_120000_00001_abcde_bucket-200_snappy.orc"));
        Assert.assertEquals(OptionalInt.of(300),
                HiveBucketingUtil.getBucketNumber("20230101_120000_00001_abcde_bucket-300.gz"));
        Assert.assertEquals(OptionalInt.of(0),
                HiveBucketingUtil.getBucketNumber("20230101_120000_00001_abcde_bucket-000"));
        Assert.assertEquals(OptionalInt.of(999),
                HiveBucketingUtil.getBucketNumber("20230101_120000_00001_abcde_bucket-999"));

        Assert.assertEquals(OptionalInt.of(0),
                HiveBucketingUtil.getBucketNumber("0"));
        Assert.assertEquals(OptionalInt.of(1),
                HiveBucketingUtil.getBucketNumber("1"));
        Assert.assertEquals(OptionalInt.of(42),
                HiveBucketingUtil.getBucketNumber("42"));
        Assert.assertEquals(OptionalInt.of(100),
                HiveBucketingUtil.getBucketNumber("100"));
        Assert.assertEquals(OptionalInt.of(999),
                HiveBucketingUtil.getBucketNumber("999"));
        Assert.assertEquals(OptionalInt.of(1000),
                HiveBucketingUtil.getBucketNumber("1000"));

        Assert.assertEquals(OptionalInt.empty(), HiveBucketingUtil.getBucketNumber("data_file.parquet"));
        Assert.assertEquals(OptionalInt.empty(), HiveBucketingUtil.getBucketNumber("bucket-123"));
        Assert.assertEquals(OptionalInt.empty(),
                HiveBucketingUtil.getBucketNumber("20230101_120000_00001_abcde_bucket001"));
        Assert.assertEquals(OptionalInt.empty(), HiveBucketingUtil.getBucketNumber("000abc_0"));
        Assert.assertEquals(OptionalInt.empty(), HiveBucketingUtil.getBucketNumber("123_file"));
    }


    @Test
    public void testFilterByBucketConjuncts(@Injectable SlotRef slotRef) {
        HiveBucketingUtil.HiveBucketProperty hiveBucketProperty1 =
                new HiveBucketingUtil.HiveBucketProperty(Lists.newArrayList("test1"), 500,
                HiveBucketingUtil.BucketingVersion.BUCKETING_V1);
        new MockUp<SlotRef>() {
            @Mock
            public String getColumnName() {
                return "test1";
            }

            @Mock
            public Type getType() {
                return Type.STRING;
            }
        };
        List<HiveMetaStoreCache.FileCacheValue> fileCacheValuesList = new ArrayList<>();
        HiveMetaStoreCache.FileCacheValue fileCacheValue = new HiveMetaStoreCache.FileCacheValue();
        HiveMetaStoreCache.HiveFileStatus status1 = new HiveMetaStoreCache.HiveFileStatus();
        status1.setPath(new LocationPath("hdfs://nn1/path/to/00042_0.parquet"));
        HiveMetaStoreCache.HiveFileStatus status2 = new HiveMetaStoreCache.HiveFileStatus();
        status2.setPath(new LocationPath("hdfs://nn1/path/to/00043_0.parquet"));
        HiveMetaStoreCache.HiveFileStatus status3 = new HiveMetaStoreCache.HiveFileStatus();
        status3.setPath(new LocationPath("hdfs://nn1/path/to/000418_0.parquet"));
        fileCacheValue.getFiles().add(status1);
        fileCacheValue.getFiles().add(status2);
        fileCacheValue.getFiles().add(status3);
        fileCacheValue.setAcidInfo(new AcidInfo("", Lists.newArrayList()));
        fileCacheValue.setSplittable(true);
        fileCacheValue.setPartitionValues(Lists.newArrayList());
        fileCacheValuesList.add(fileCacheValue);
        BinaryPredicate binaryPredicate = new BinaryPredicate(BinaryPredicate.Operator.EQ, slotRef,
                new StringLiteral("测试"));
        List<Expr> exprList = new ArrayList<>();
        exprList.add(binaryPredicate);
        List<HiveMetaStoreCache.FileCacheValue> newFileCacheValuesList =
                HiveBucketingUtil.filterByBucketConjuncts(hiveBucketProperty1, exprList, fileCacheValuesList);
        Assert.assertEquals(1, newFileCacheValuesList.size());
        Assert.assertEquals(1, newFileCacheValuesList.get(0).getFiles().size());
        Assert.assertEquals("000418_0.parquet",
                newFileCacheValuesList.get(0).getFiles().get(0).getPath().getPath().getName());

        HiveBucketingUtil.HiveBucketProperty hiveBucketProperty2 =
                new HiveBucketingUtil.HiveBucketProperty(Lists.newArrayList("test1", "test2"), 500,
                HiveBucketingUtil.BucketingVersion.BUCKETING_V1);
        newFileCacheValuesList =
                HiveBucketingUtil.filterByBucketConjuncts(hiveBucketProperty2, exprList, fileCacheValuesList);
        Assert.assertEquals(1, newFileCacheValuesList.size());
        Assert.assertEquals(3, newFileCacheValuesList.get(0).getFiles().size());

        HiveBucketingUtil.HiveBucketProperty hiveBucketProperty3 =
                new HiveBucketingUtil.HiveBucketProperty(Lists.newArrayList("test"), 500,
                HiveBucketingUtil.BucketingVersion.BUCKETING_V1);
        newFileCacheValuesList =
                HiveBucketingUtil.filterByBucketConjuncts(hiveBucketProperty3, exprList, fileCacheValuesList);
        Assert.assertEquals(1, newFileCacheValuesList.size());
        Assert.assertEquals(3, newFileCacheValuesList.get(0).getFiles().size());

    }

}
