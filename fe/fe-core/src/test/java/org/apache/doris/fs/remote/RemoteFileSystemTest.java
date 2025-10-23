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

package org.apache.doris.fs.remote;

import org.apache.doris.fs.HdfsAuditUtil;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class RemoteFileSystemTest {

    @Test
    public void testGetLocatedFilesWithAuditContext() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        FileSystem mockFileSystem = Mockito.mock(FileSystem.class);
        Path testPath = new Path("/test");
        boolean recursive = false;

        @SuppressWarnings("unchecked")
        RemoteIterator<LocatedFileStatus> mockIterator = Mockito.mock(RemoteIterator.class);

        try (MockedStatic<HdfsAuditUtil> mockedStatic = Mockito.mockStatic(HdfsAuditUtil.class)) {
            Map<String, String> mockAuditCtx = new HashMap<>();
            mockAuditCtx.put("businessId", "test");
            mockAuditCtx.put("erp", "test_user");
            mockAuditCtx.put("source", "doris");

            mockedStatic.when(HdfsAuditUtil::buildAuditContextMap).thenReturn(mockAuditCtx);

            Mockito.when(mockFileSystem.listFilesWithAuditContext(Mockito.any(Path.class),
                    Mockito.any(boolean.class), Mockito.any(Map.class)))
                    .thenReturn(mockIterator);

            Map<String, String> properties = new HashMap<>();
            DFSFileSystem dfsFileSystem = new DFSFileSystem(properties);

            Method method = RemoteFileSystem.class.getDeclaredMethod(
                    "getLocatedFiles", boolean.class, FileSystem.class, Path.class);
            method.setAccessible(true);

            RemoteIterator<LocatedFileStatus> result = (RemoteIterator<LocatedFileStatus>)
                    method.invoke(dfsFileSystem, recursive, mockFileSystem, testPath);

            Assertions.assertNotNull(result);
            Mockito.verify(mockFileSystem).listFilesWithAuditContext(Mockito.any(Path.class),
                    Mockito.any(boolean.class), Mockito.any(Map.class));
        }
    }

    @Test
    public void testGetFileStatusesWithAuditContext() throws IOException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        FileSystem mockFileSystem = Mockito.mock(FileSystem.class);
        String remotePath = "/test";
        FileStatus mockFileStatus = Mockito.mock(FileStatus.class);

        try (MockedStatic<HdfsAuditUtil> mockedStatic = Mockito.mockStatic(HdfsAuditUtil.class)) {
            Map<String, String> mockAuditCtx = new HashMap<>();
            mockAuditCtx.put("businessId", "test");
            mockAuditCtx.put("erp", "test_user");
            mockAuditCtx.put("source", "doris");

            mockedStatic.when(HdfsAuditUtil::buildAuditContextMap).thenReturn(mockAuditCtx);

            Mockito.when(mockFileSystem.listStatusWithAuditContext(Mockito.any(Path.class), Mockito.any(Map.class)))
                    .thenReturn(new FileStatus[]{mockFileStatus});

            Map<String, String> properties = new HashMap<>();
            DFSFileSystem dfsFileSystem = new DFSFileSystem(properties);

            // 使用反射调用受保护的方法
            Method method = RemoteFileSystem.class.getDeclaredMethod(
                    "getFileStatuses", String.class, FileSystem.class);
            method.setAccessible(true);

            FileStatus[] result = (FileStatus[]) method.invoke(dfsFileSystem, remotePath, mockFileSystem);

            Assertions.assertNotNull(result);
            Assertions.assertEquals(1, result.length);
            Mockito.verify(mockFileSystem).listStatusWithAuditContext(Mockito.any(Path.class), Mockito.any(Map.class));
        }
    }
}
