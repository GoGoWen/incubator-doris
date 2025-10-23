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
import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreCache.FileCacheValue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hive.common.util.Murmur3;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveBucketingUtil {
    private static final Logger LOG = LogManager.getLogger(HiveBucketingUtil.class);

    private static final Iterable<Pattern> BUCKET_PATTERNS = ImmutableList.of(
            // Hive naming pattern per `org.apache.hadoop.hive.ql.exec.Utilities#getBucketIdFromFile()`
            Pattern.compile("(0\\d+)_\\d+.*"),
            // legacy Presto naming pattern (current version matches Hive)
            Pattern.compile("\\d{8}_\\d{6}_\\d{5}_[a-z0-9]{5}_bucket-(\\d+)(?:[-_.].*)?"));

    public enum BucketingVersion {
        BUCKETING_V1(1) {
            @Override
            int hash(Type type, Object value) {
                if (value == null) {
                    return 0;
                }
                if (type.equals(Type.BOOLEAN)) {
                    return (boolean) value ? 1 : 0;
                } else if (type.equals(Type.TINYINT)) {
                    return SignedBytes.checkedCast((long) value);
                } else if (type.equals(Type.SMALLINT)) {
                    return Shorts.checkedCast((long) value);
                } else if (type.equals(Type.INT)) {
                    return Math.toIntExact((long) value);
                } else if (type.equals(Type.BIGINT)) {
                    long bigintValue = (long) value;
                    return (int) ((bigintValue >>> 32) ^ bigintValue);
                } else if (type.equals(Type.STRING)) {
                    return hashBytes(0, ((String) value).getBytes(StandardCharsets.UTF_8));
                } else if (type.equals(Type.VARCHAR)) {
                    return hashBytes(1, ((String) value).getBytes(StandardCharsets.UTF_8));
                }
                throw new UnsupportedOperationException(
                    "Computation of Hive bucket hashCode is not supported for Hive primitive category: "
                        + type.toSql());
            }

            int hashBytes(int initialValue, byte[] bytes) {
                int result = initialValue;
                for (int i = 0; i < bytes.length; i++) {
                    result = result * 31 + bytes[i];
                }
                return result;
            }
        },
        BUCKETING_V2(2) {
            @Override
            int hash(Type type, Object value) {
                if (value == null) {
                    return 0;
                }
                if (type.equals(Type.BOOLEAN)) {
                    return (boolean) value ? 1 : 0;
                } else if (type.equals(Type.TINYINT)) {
                    return SignedBytes.checkedCast((long) value);
                } else if (type.equals(Type.SMALLINT)) {
                    return Murmur3.hash32(bytes(Shorts.checkedCast((long) value)));
                } else if (type.equals(Type.INT)) {
                    return Murmur3.hash32(bytes(Math.toIntExact((long) value)));
                } else if (type.equals(Type.BIGINT)) {
                    return Murmur3.hash32(bytes((long) value));
                } else if (type.equals(Type.STRING)) {
                    return Murmur3.hash32(((String) value).getBytes(StandardCharsets.UTF_8));
                } else if (type.equals(Type.VARCHAR)) {
                    return Murmur3.hash32(((String) value).getBytes(StandardCharsets.UTF_8));
                }
                throw new UnsupportedOperationException(
                    "Computation of Hive bucket hashCode is not supported for Hive primitive category: "
                        + type.toSql());
            }

            // big-endian
            @SuppressWarnings("NumericCastThatLosesPrecision")
            protected byte[] bytes(short value) {
                return new byte[] {(byte) ((value >> 8) & 0xff), (byte) (value & 0xff)};
            }

            // big-endian
            @SuppressWarnings("NumericCastThatLosesPrecision")
            private byte[] bytes(int value) {
                return new byte[] {(byte) ((value >> 24) & 0xff), (byte) ((value >> 16) & 0xff),
                    (byte) ((value >> 8) & 0xff), (byte) (value & 0xff)};
            }

            // big-endian
            @SuppressWarnings("NumericCastThatLosesPrecision")
            private byte[] bytes(long value) {
                return new byte[] {
                    (byte) ((value >> 56) & 0xff), (byte) ((value >> 48) & 0xff),
                    (byte) ((value >> 40) & 0xff), (byte) ((value >> 32) & 0xff),
                    (byte) ((value >> 24) & 0xff), (byte) ((value >> 16) & 0xff),
                    (byte) ((value >> 8) & 0xff), (byte) (value & 0xff)};
            }
        },
        /**/;

        private final int version;

        BucketingVersion(int version) {
            this.version = version;
        }

        public int getVersion() {
            return version;
        }

        abstract int hash(Type type, Object value);
    }

    public static class HiveBucketProperty {
        private final List<String> bucketedBy;
        private final int bucketCount;
        private final BucketingVersion bucketingVersion;

        public HiveBucketProperty(List<String> bucketedBy, int bucketCount, BucketingVersion bucketingVersion) {
            this.bucketedBy = ImmutableList.copyOf(Objects.requireNonNull(bucketedBy, "bucketedBy is null"));
            this.bucketCount = bucketCount;
            this.bucketingVersion = bucketingVersion;
        }

        public List<String> getBucketedBy() {
            return bucketedBy;
        }

        public int getBucketCount() {
            return bucketCount;
        }

        public BucketingVersion getBucketingVersion() {
            return bucketingVersion;
        }
    }

    public static BucketingVersion getBucketingVersion(int bucketingVersion) {
        return bucketingVersion == 2 ? BucketingVersion.BUCKETING_V2 : BucketingVersion.BUCKETING_V1;
    }

    public static Optional<HiveBucketProperty> getHiveBucketProperty(HMSExternalTable table) throws AnalysisException {
        StorageDescriptor storageDescriptor = table.getRemoteTable().getSd();
        boolean bucketColsSet = storageDescriptor.isSetBucketCols() && !storageDescriptor.getBucketCols().isEmpty();
        boolean numBucketsSet = storageDescriptor.isSetNumBuckets() && storageDescriptor.getNumBuckets() > 0;
        if (!numBucketsSet) {
            // In Hive, a table is considered as not bucketed when its bucketCols is set but its numBucket is not set.
            return Optional.empty();
        }
        if (!bucketColsSet) {
            throw new AnalysisException("Table/partition metadata has 'numBuckets' set, but 'bucketCols' is not set: "
                            + table.getDbName() + "." + table.getName());
        }
        Map<String, String> parameters = table.getRemoteTable().getParameters();
        String bucketingVersion = parameters.getOrDefault("bucketing_version", "1");
        return Optional.of(new HiveBucketProperty(
                storageDescriptor.getBucketCols(),
                storageDescriptor.getNumBuckets(),
                getBucketingVersion(Integer.parseInt(bucketingVersion))));

    }

    public static List<FileCacheValue> filterByBucketConjuncts(HiveBucketProperty hiveBucketProperty,
            List<Expr> conjuncts, List<FileCacheValue> fileCacheValues) {
        List<String> bucketedBy = hiveBucketProperty.getBucketedBy();
        if (conjuncts.size() < bucketedBy.size()) {
            return fileCacheValues;
        }
        Map<String, Pair<Type, Object>> nameToValue = new HashMap<>();
        for (Expr expr : conjuncts) {
            if (expr instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
                if (binaryPredicate.getOp() == Operator.EQ) {
                    // slot is always on the left
                    if (binaryPredicate.getChild(0) instanceof SlotRef
                            && binaryPredicate.getChild(1) instanceof LiteralExpr) {
                        SlotRef slotRef = (SlotRef) binaryPredicate.getChild(0);
                        if (bucketedBy.contains(slotRef.getColumnName())) {
                            LiteralExpr literalExpr = (LiteralExpr) binaryPredicate.getChild(1);
                            nameToValue.put(slotRef.getColumnName(), getValueFromLiteralExpr(slotRef.getType(),
                                    literalExpr));
                        }
                    }
                }
            }
        }
        if (nameToValue.size() != bucketedBy.size()) {
            return fileCacheValues;
        }
        Type[] types = new Type[bucketedBy.size()];
        Object[] values = new Object[bucketedBy.size()];
        int i = 0;
        for (String name : bucketedBy) {
            Pair<Type, Object> result = nameToValue.get(name);
            types[i] = result.getKey();
            values[i] = result.getValue();
            i++;
        }
        try {
            BucketingVersion bucketingVersion = hiveBucketProperty.getBucketingVersion();
            int bucketHashCode = getBucketHashCode(types, values, bucketingVersion);
            int bucketNumber = (bucketHashCode & Integer.MAX_VALUE)
                    % hiveBucketProperty.getBucketCount();
            return getFilterFileCacheValueList(fileCacheValues, bucketNumber);
        } catch (Exception e) {
            LOG.warn("failed to get bucket hash code", e);
            return fileCacheValues;
        }
    }

    @VisibleForTesting
    protected static List<FileCacheValue> getFilterFileCacheValueList(List<FileCacheValue> fileCacheValues,
            int bucketNumber) {
        List<FileCacheValue> filterFileCacheValueList = Lists.newArrayListWithCapacity(fileCacheValues.size());
        for (FileCacheValue fileCacheValue : fileCacheValues) {
            FileCacheValue cacheValue = new FileCacheValue();
            cacheValue.setSplittable(fileCacheValue.isSplittable());
            cacheValue.setPartitionValues(fileCacheValue.getPartitionValues());
            cacheValue.setAcidInfo(fileCacheValue.getAcidInfo());
            fileCacheValue.getFiles().forEach(file -> {
                OptionalInt intOptional = getBucketNumber(file.getPath().getPath().getName());
                if (intOptional.isPresent() && bucketNumber == intOptional.getAsInt()) {
                    cacheValue.getFiles().add(file);
                }
            });
            filterFileCacheValueList.add(cacheValue);
        }
        return filterFileCacheValueList;
    }


    @VisibleForTesting
    protected static OptionalInt getBucketNumber(String fileName) {
        for (Pattern pattern : BUCKET_PATTERNS) {
            Matcher matcher = pattern.matcher(fileName);
            if (matcher.matches()) {
                return OptionalInt.of(Integer.parseInt(matcher.group(1)));
            }
        }
        // Numerical file name when "file_renaming_enabled" is true
        if (fileName.matches("\\d+")) {
            return OptionalInt.of(Integer.parseInt(fileName));
        }

        return OptionalInt.empty();
    }

    @VisibleForTesting
    protected static Pair<Type, Object> getValueFromLiteralExpr(Type type, LiteralExpr literalExpr) {
        return Pair.of(type, literalExpr.getRealValue());
    }

    @VisibleForTesting
    protected static int getBucketHashCode(Type[] types, Object[] values, BucketingVersion bucketingVersion) {
        Preconditions.checkArgument(types.length == values.length);
        int result = 0;
        for (int i = 0; i < values.length; i++) {
            int fieldHash = bucketingVersion.hash(types[i], values[i]);
            result = result * 31 + fieldHash;
        }
        return result;
    }
}
