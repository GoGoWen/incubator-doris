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

package org.apache.doris.fs.operations;

import org.apache.doris.backup.Status;
import org.apache.doris.fs.HdfsAuditUtil;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

public class HDFSFileOperationsTest {

    @Test
    public void testOpenReaderWithAuditContext() throws Exception {
        FileSystem mockFileSystem = Mockito.mock(FileSystem.class);
        HDFSFileOperations operations = new HDFSFileOperations(mockFileSystem);
        HDFSOpParams params = OpParams.of("hdfs://localhost:9000/test.txt");

        FSDataInputStream mockInputStream = Mockito.mock(FSDataInputStream.class);

        try (MockedStatic<HdfsAuditUtil> mockedStatic = Mockito.mockStatic(HdfsAuditUtil.class)) {
            Map<String, String> mockAuditCtx = new HashMap<>();
            mockAuditCtx.put("businessId", "test");
            mockAuditCtx.put("erp", "test_user");
            mockAuditCtx.put("source", "doris");

            mockedStatic.when(HdfsAuditUtil::buildAuditContextMap).thenReturn(mockAuditCtx);

            Mockito.when(mockFileSystem.openWithAuditContext(
                    Mockito.any(Path.class), Mockito.anyInt(), Mockito.eq(mockAuditCtx)))
                    .thenReturn(mockInputStream);

            Status status = operations.openReader(params);

            Assertions.assertEquals(Status.OK, status);
            Mockito.verify(mockFileSystem).openWithAuditContext(
                    Mockito.any(Path.class), Mockito.anyInt(), Mockito.eq(mockAuditCtx));
        }
    }
}
