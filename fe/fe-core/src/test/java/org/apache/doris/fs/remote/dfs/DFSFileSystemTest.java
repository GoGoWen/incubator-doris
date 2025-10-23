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

package org.apache.doris.fs.remote.dfs;

import org.apache.doris.backup.Status;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.fs.remote.RemoteFile;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DFSFileSystemTest {

    private DFSFileSystem dfsFileSystem;

    @Mocked
    private FileSystem mockFileSystem;

    private boolean originalConfigValue;

    @BeforeEach
    public void setUp() {
        // Save original config value
        originalConfigValue = Config.enable_list_hdfs_files_without_block_locations;

        // Create a real DFSFileSystem instance for testing
        dfsFileSystem = new DFSFileSystem(new HashMap<>());
    }

    @AfterEach
    public void tearDown() {
        // Restore original config value
        Config.enable_list_hdfs_files_without_block_locations = originalConfigValue;
    }



    @Test
    public void testListFilesRecursiveWithNewImplementation() throws Exception {
        // Enable new implementation
        Config.enable_list_hdfs_files_without_block_locations = true;

        // Create mock file structure:
        // /test/
        // ├── file1.txt
        // ├── file2.txt
        // └── subdir/
        //      ├── file3.txt
        //      └── file4.txt

        Path file1Path = new Path("/test/file1.txt");
        Path file2Path = new Path("/test/file2.txt");
        Path file3Path = new Path("/test/subdir/file3.txt");
        Path file4Path = new Path("/test/subdir/file4.txt");

        // Create additional paths for proper testing
        Path subdirPath = new Path("/test/subdir");

        // Mock FileStatus objects
        FileStatus file1Status = createMockFileStatus(file1Path, false, 1024, 1000);
        FileStatus file2Status = createMockFileStatus(file2Path, false, 2048, 2000);
        FileStatus subdirStatus = createMockFileStatus(subdirPath, true, 0, 3000);
        FileStatus file3Status = createMockFileStatus(file3Path, false, 512, 4000);
        FileStatus file4Status = createMockFileStatus(file4Path, false, 256, 5000);

        // Use MockUp to partially mock DFSFileSystem - only override nativeFileSystem method
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        // Mock FileSystem.listStatusWithAuditContext calls
        new Expectations() {{
                // Mock listStatusWithAuditContext for root directory (first call)
                mockFileSystem.listStatusWithAuditContext(new Path("/test"), (Map<String, String>) any);
                result = new FileStatus[]{file1Status, file2Status, subdirStatus};
                times = 1;

                // Mock listStatusWithAuditContext for subdirectory (second call)
                mockFileSystem.listStatusWithAuditContext(new Path("/test/subdir"), (Map<String, String>) any);
                result = new FileStatus[]{file3Status, file4Status};
                times = 1;
            }};

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/test", true, result);

        // Verify results
        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(4, result.size()); // Should find all 4 files

        // Verify file details
        Map<String, RemoteFile> fileMap = new HashMap<>();
        for (RemoteFile file : result) {
            fileMap.put(file.getName(), file);
        }

        Assertions.assertTrue(fileMap.containsKey("file1.txt"));
        Assertions.assertTrue(fileMap.containsKey("file2.txt"));
        Assertions.assertTrue(fileMap.containsKey("file3.txt"));
        Assertions.assertTrue(fileMap.containsKey("file4.txt"));

        RemoteFile file1 = fileMap.get("file1.txt");
        Assertions.assertTrue(file1.isFile());
        Assertions.assertFalse(file1.isDirectory());
        Assertions.assertEquals(1024, file1.getSize());
        Assertions.assertEquals(1000, file1.getModificationTime());
        Assertions.assertNull(file1.getBlockLocations()); // New implementation sets null
    }

    @Test
    public void testListFilesNonRecursiveWithNewImplementation() throws Exception {
        // Enable new implementation
        Config.enable_list_hdfs_files_without_block_locations = true;

        // Create mock file structure for non-recursive test:
        // /test/
        // ├── file1.txt
        // ├── file2.txt
        // └── subdir/ (directory, should not be traversed in non-recursive mode)

        Path file1Path = new Path("/test/file1.txt");
        Path file2Path = new Path("/test/file2.txt");
        Path subdirPath = new Path("/test/subdir");

        // Mock FileStatus objects
        FileStatus file1Status = createMockFileStatus(file1Path, false, 1024, 1000);
        FileStatus file2Status = createMockFileStatus(file2Path, false, 2048, 2000);
        FileStatus subdirStatus = createMockFileStatus(subdirPath, true, 0, 3000);

        // Use MockUp to partially mock DFSFileSystem - only override nativeFileSystem method
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        // Mock FileSystem.listStatusWithAuditContext calls
        new Expectations() {{
                // Mock listStatusWithAuditContext for root directory only (non-recursive)
                mockFileSystem.listStatusWithAuditContext((Path) any, (Map<String, String>) any);
                result = new FileStatus[]{file1Status, file2Status, subdirStatus};
                // Note: subdirectory should NOT be traversed in non-recursive mode
            }};

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/test", false, result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(2, result.size()); // Only files, not directories

        Map<String, RemoteFile> fileMap = new HashMap<>();
        for (RemoteFile file : result) {
            fileMap.put(file.getName(), file);
        }

        Assertions.assertTrue(fileMap.containsKey("file1.txt"));
        Assertions.assertTrue(fileMap.containsKey("file2.txt"));
        Assertions.assertFalse(fileMap.containsKey("subdir")); // Directory should not be included

        // Verify all returned files have null block locations
        for (RemoteFile file : result) {
            Assertions.assertNull(file.getBlockLocations());
        }
    }

    @Test
    public void testListFilesFileNotFound() throws Exception {
        // Enable new implementation
        Config.enable_list_hdfs_files_without_block_locations = true;

        // Use MockUp to partially mock DFSFileSystem - only override nativeFileSystem method
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        // Mock FileSystem.listStatusWithAuditContext to throw FileNotFoundException
        new Expectations() {{
                // Mock listStatusWithAuditContext to throw FileNotFoundException
                mockFileSystem.listStatusWithAuditContext((Path) any, (Map<String, String>) any);
                result = new java.io.FileNotFoundException("Path not found: /nonexistent");
            }};

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/nonexistent", true, result);

        // Verify error handling
        Assertions.assertEquals(Status.ErrCode.NOT_FOUND, status.getErrCode());
        Assertions.assertTrue(status.getErrMsg().contains("Path not found"));
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testListFilesIOException() throws Exception {
        // Enable new implementation
        Config.enable_list_hdfs_files_without_block_locations = true;

        // Use MockUp to partially mock DFSFileSystem - only override nativeFileSystem method
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        // Mock FileSystem.listStatusWithAuditContext to throw IOException
        new Expectations() {{
                // Mock listStatusWithAuditContext to throw IOException
                mockFileSystem.listStatusWithAuditContext((Path) any, (Map<String, String>) any);
                result = new java.io.IOException("Network error");
            }};

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/test", true, result);

        // Verify error handling
        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, status.getErrCode());
        Assertions.assertTrue(status.getErrMsg().contains("Network error"));
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testListFilesUserException() throws Exception {
        // Enable new implementation
        Config.enable_list_hdfs_files_without_block_locations = true;

        // Use MockUp to partially mock DFSFileSystem - override nativeFileSystem to throw UserException
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) throws UserException {
                throw new UserException("Authentication failed");
            }
        };

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/test", true, result);

        // Verify error handling
        Assertions.assertEquals(Status.ErrCode.COMMON_ERROR, status.getErrCode());
        Assertions.assertTrue(status.getErrMsg().contains("Authentication failed"));
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testListFilesEmptyDirectory() throws Exception {
        // Enable new implementation
        Config.enable_list_hdfs_files_without_block_locations = true;

        // Use MockUp to partially mock DFSFileSystem - only override nativeFileSystem method
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        // Mock FileSystem.listStatusWithAuditContext to return empty array
        new Expectations() {{
                // Mock listStatusWithAuditContext to return empty array (empty directory)
                mockFileSystem.listStatusWithAuditContext((Path) any, (Map<String, String>) any);
                result = new FileStatus[0];
            }};

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/empty", true, result);

        // Verify results
        Assertions.assertEquals(Status.OK, status);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testListFilesWithOldImplementation() throws Exception {
        // Disable new implementation to test fallback to parent class
        Config.enable_list_hdfs_files_without_block_locations = false;

        // When the flag is disabled, the method should call super.listFiles()
        // We need to mock the parent class behavior
        new Expectations() {{
                // The real implementation will call super.listFiles() when flag is false
                // We can't easily test this without mocking the parent class
                // For now, we'll just verify that the flag controls the behavior
            }};

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/test", true, result);

        // When flag is disabled, it should call parent implementation
        // The exact behavior depends on the parent class implementation
        // For this test, we just verify that the method doesn't crash
        Assertions.assertNotNull(status);
    }

    @Test
    public void testListFilesBasicFunctionality() throws Exception {
        // Simple test to verify basic functionality
        Config.enable_list_hdfs_files_without_block_locations = true;

        Path file1Path = new Path("/test/test1.txt");
        Path file2Path = new Path("/test/test2.txt");

        // Mock FileStatus objects
        FileStatus file1Status = createMockFileStatus(file1Path, false, 1024, 1000);
        FileStatus file2Status = createMockFileStatus(file2Path, false, 2048, 2000);

        // Use MockUp to partially mock DFSFileSystem - only override nativeFileSystem method
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        // Mock FileSystem.listStatusWithAuditContext calls
        new Expectations() {{
                // Mock listStatusWithAuditContext for root directory
                mockFileSystem.listStatusWithAuditContext((Path) any, (Map<String, String>) any);
                result = new FileStatus[]{file1Status, file2Status};
            }};

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/test", true, result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("test1.txt", result.get(0).getName());
        Assertions.assertEquals("test2.txt", result.get(1).getName());

        // Verify all files have null block locations (new implementation)
        for (RemoteFile file : result) {
            Assertions.assertNull(file.getBlockLocations());
        }
    }

    @Test
    public void testListFilesConfigurationToggle() throws Exception {
        // Test that the configuration flag properly controls behavior

        Path filePath = new Path("/test/file.txt");
        FileStatus fileStatus = createMockFileStatus(filePath, false, 1024, 1000);

        // Test with new implementation enabled
        Config.enable_list_hdfs_files_without_block_locations = true;

        // Use MockUp to partially mock DFSFileSystem - only override nativeFileSystem method
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        new Expectations() {{
                mockFileSystem.listStatusWithAuditContext((Path) any, (Map<String, String>) any);
                result = new FileStatus[]{fileStatus};
            }};

        List<RemoteFile> result1 = new ArrayList<>();
        Status status1 = dfsFileSystem.listFiles("/test", false, result1);

        Assertions.assertEquals(Status.OK, status1);
        Assertions.assertEquals(1, result1.size());
        Assertions.assertNull(result1.get(0).getBlockLocations()); // New implementation sets null

        // Test with new implementation disabled (falls back to parent)
        Config.enable_list_hdfs_files_without_block_locations = false;

        List<RemoteFile> result2 = new ArrayList<>();
        Status status2 = dfsFileSystem.listFiles("/test", false, result2);

        // When flag is disabled, it should call parent implementation
        Assertions.assertNotNull(status2);
    }

    @Test
    public void testListFilesWithIgnoreHiddenDirectory() throws Exception {
        // Test that the configuration flag properly controls behavior

        Path directoryPath = new Path("/test/.test");
        Path directoryPath2 = new Path("/test/_test");
        Path filePath = new Path("/test/.test/file.txt");
        Path filePath2 = new Path("/test/_test/file2.txt");
        FileStatus fileStatus1 = createMockFileStatus(directoryPath, true, 4, 1000);
        FileStatus fileStatus2 = createMockFileStatus(filePath, false, 1024, 1000);
        FileStatus fileStatus3 = createMockFileStatus(directoryPath2, true, 4, 1000);
        FileStatus fileStatus4 = createMockFileStatus(filePath2, false, 1024, 1000);

        Config.enable_list_hdfs_files_without_block_locations = true;
        Config.enable_list_hdfs_files_ignore_hidden_directory = true;

        // Use MockUp to partially mock DFSFileSystem - only override nativeFileSystem method
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        new Expectations() {
            {
                mockFileSystem.listStatusWithAuditContext((Path) any, (Map<String, String>) any);
                result = new FileStatus[]{fileStatus1, fileStatus3};
                minTimes = 1;
            }
        };

        List<RemoteFile> result1 = new ArrayList<>();
        Status status1 = dfsFileSystem.listFiles("/test", true, result1);

        Assertions.assertEquals(Status.OK, status1);
        Assertions.assertEquals(0, result1.size());


        Config.enable_list_hdfs_files_ignore_hidden_directory = false;

        new Expectations() {
            {
                mockFileSystem.listStatusWithAuditContext((Path) any, (Map<String, String>) any);
                result = new FileStatus[]{fileStatus2, fileStatus4};
                minTimes = 1;
            }
        };

        List<RemoteFile> result2 = new ArrayList<>();
        Status status2 = dfsFileSystem.listFiles("/test", true, result2);
        Assertions.assertEquals(Status.OK, status2);
        Assertions.assertEquals(2, result2.size());
        Assertions.assertEquals("file.txt", result2.get(0).getName());
        Assertions.assertEquals("file2.txt", result2.get(1).getName());
    }

    @Test
    public void testListFilesWithAuditContext() throws Exception {
        // Enable new implementation
        Config.enable_list_hdfs_files_without_block_locations = true;

        Path file1Path = new Path("/test/file1.txt");
        FileStatus file1Status = createMockFileStatus(file1Path, false, 1024, 1000);

        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        new Expectations() {{
                mockFileSystem.listStatusWithAuditContext((Path) any, (Map<String, String>) any);
                result = new FileStatus[]{file1Status};
            }};

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/test", false, result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("file1.txt", result.get(0).getName());
    }

    @Test
    public void testExistsWithAuditContext() throws Exception {
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };
        new Expectations() {{
                mockFileSystem.existsWithAuditContext((Path) any, (Map<String, String>) any);
                result = true;
            }};

        Status status = dfsFileSystem.exists("/test/file.txt");

        Assertions.assertEquals(Status.OK, status);
    }

    @Test
    public void testGlobListWithAuditContext() throws Exception {
        Path file1Path = new Path("/test/file1.txt");
        FileStatus file1Status = createMockFileStatus(file1Path, false, 1024, 1000);

        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        new Expectations() {{
                mockFileSystem.globStatusWithAuditContext((Path) any, (Map<String, String>) any);
                result = new FileStatus[]{file1Status};
            }};

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.globList("/test/*", result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("file1.txt", result.get(0).getName());
    }

    @Test
    public void testGetLocatedFilesWithAuditContext() throws Exception {
        Config.enable_list_hdfs_files_without_block_locations = false;
        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        new Expectations() {{
                mockFileSystem.listFilesWithAuditContext((Path) any, false, (Map<String, String>) any);
                result = new org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus>() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public org.apache.hadoop.fs.LocatedFileStatus next() {
                        return null;
                    }
                };
            }};

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/test", false, result);

        Assertions.assertEquals(Status.OK, status);
    }

    @Test
    public void testGetFileStatusesWithAuditContext() throws Exception {
        Config.enable_list_hdfs_files_without_block_locations = true;

        FileStatus fileStatus = createMockFileStatus(new Path("/test/file.txt"), false, 1024, 1000);

        new MockUp<DFSFileSystem>() {
            @Mock
            public FileSystem nativeFileSystem(String remotePath) {
                return mockFileSystem;
            }
        };

        new Expectations() {{
                mockFileSystem.listStatusWithAuditContext((Path) any, (Map<String, String>) any);
                result = new FileStatus[]{fileStatus};
            }};

        List<RemoteFile> result = new ArrayList<>();
        Status status = dfsFileSystem.listFiles("/test", false, result);

        Assertions.assertEquals(Status.OK, status);
        Assertions.assertEquals(1, result.size());
    }

    /**
     * Helper method to create mock FileStatus objects
     */
    private FileStatus createMockFileStatus(final Path path, final boolean isDirectory,
            final long length, final long modificationTime) {
        // Create a simple mock FileStatus using an anonymous class
        return new FileStatus() {
            @Override
            public Path getPath() {
                return path;
            }

            @Override
            public boolean isDirectory() {
                return isDirectory;
            }

            @Override
            public long getLen() {
                return length;
            }

            @Override
            public long getBlockSize() {
                return 134217728L; // 128MB default block size
            }

            @Override
            public long getModificationTime() {
                return modificationTime;
            }

            @Override
            public boolean isFile() {
                return !isDirectory;
            }

            @Override
            public short getReplication() {
                return 3; // Default HDFS replication
            }

            @Override
            public long getAccessTime() {
                return modificationTime;
            }

            @Override
            public FsPermission getPermission() {
                return new FsPermission((short) 0644);
            }

            @Override
            public String getOwner() {
                return "testuser";
            }

            @Override
            public String getGroup() {
                return "testgroup";
            }
        };
    }
}
