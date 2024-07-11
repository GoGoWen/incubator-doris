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

package org.apache.doris.catalog;

import mockit.Expectations;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.CountingDataOutputStream;
import org.apache.doris.load.Load;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.persist.meta.MetaHeader;
import org.apache.hadoop.shaded.org.apache.commons.io.output.DeferredFileOutputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.Field;
import java.util.Random;
import java.util.zip.*;

public class EnvEncodeTest {

    @Before
    public void setUp() {
        MetaContext metaContext = new MetaContext();
        new Expectations(metaContext) {
            {
                MetaContext.get();
                minTimes = 0;
                result = metaContext;
            }
        };
    }

    public void mkdir(String dirString) {
        File dir = new File(dirString);
        if (!dir.exists()) {
            dir.mkdir();
        } else {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }
        }
    }

    public void deleteDir(String metaDir) {
        File dir = new File(metaDir);
        if (dir.exists()) {
            File[] files = dir.listFiles();
            for (File file : files) {
                if (file.isFile()) {
                    file.delete();
                }
            }

            dir.delete();
        }
    }

    @Test
    public void testSaveLoadHeader() throws Exception {
        String dir = "testLoadHeader";
        mkdir(dir);
        File file = new File(dir, "image");
        file.createNewFile();
        Deflater deflater = new Deflater();
        CountingDataOutputStream dos = new CountingDataOutputStream( new DeflaterOutputStream(
                new FileOutputStream(file), deflater));
        Env env = Env.getCurrentEnv();
        MetaContext.get().setMetaVersion(FeConstants.meta_version);
        Field field = env.getClass().getDeclaredField("load");
        field.setAccessible(true);
        field.set(env, new Load());

        long checksum1 = env.saveHeader(dos, new Random().nextLong(), 0);
        env.clear();
        env = null;
        dos.close();

        Inflater inflater = new Inflater();
        DataInputStream dis = new DataInputStream(new InflaterInputStream(
                new BufferedInputStream(new FileInputStream(file)), inflater));
        env = Env.getCurrentEnv();
        long checksum2 = env.loadHeader(dis, MetaHeader.EMPTY_HEADER, 0);
        Assert.assertEquals(checksum1, checksum2);
        dis.close();

        deleteDir(dir);
    }
}
