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

package org.apache.doris.hudi;

import org.apache.doris.common.classloader.ThreadClassLoaderContext;
import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * RowData represents a single row of data read from Hudi reader.
 */
private static class RowData {
    private final NullWritable key;
    private final ArrayWritable value;
    
    public RowData(NullWritable key, ArrayWritable value) {
        this.key = key;
        this.value = value;
    }
    
    public NullWritable getKey() {
        return key;
    }
    
    public ArrayWritable getValue() {
        return value;
    }
}

/**
 * HadoopHudiJniScanner is a JniScanner implementation that reads Hudi data using hudi-hadoop-mr.
 */
public class HadoopHudiJniScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopHudiJniScanner.class);

    private static final String HADOOP_CONF_PREFIX = "hadoop_conf.";
    
    // Shared thread pool for background readers across all scanner instances
    private static final ThreadPoolExecutor BACKGROUND_READER_POOL = new ThreadPoolExecutor(
            2, // core pool size
            10, // maximum pool size  
            60L, TimeUnit.SECONDS, // keep alive time
            new LinkedBlockingQueue<>(100), // work queue
            r -> {
                Thread t = new Thread(r, "HadoopHudiJniScanner-BackgroundReader-" + System.currentTimeMillis());
                t.setDaemon(true);
                return t;
            }
    );

    // Hudi data info
    private final String basePath;
    private final String dataFilePath;
    private final long dataFileLength;
    private final String[] deltaFilePaths;
    private final String instantTime;
    private final String serde;
    private final String inputFormat;

    // schema info
    private final String hudiColumnNames;
    private final String[] hudiColumnTypes;
    private final String[] requiredFields;
    private int[] requiredColumnIds;
    private ColumnType[] requiredTypes;

    // Hadoop info
    private RecordReader<NullWritable, ArrayWritable> reader;
    private StructObjectInspector rowInspector;
    private final ObjectInspector[] fieldInspectors;
    private final StructField[] structFields;
    private Deserializer deserializer;
    private final Map<String, String> fsOptionsProps;

    // scanner info
    private final HadoopHudiColumnValue columnValue;
    private final int fetchSize;
    private final ClassLoader classLoader;

    private final String hadoopUserName;
    private final String hadoopUserToken;

    // Threading related fields
    private BlockingQueue<RowData> dataQueue;
    private java.util.concurrent.Future<?> backgroundReaderTask;
    private final AtomicBoolean isReaderFinished = new AtomicBoolean(false);
    private final AtomicReference<Exception> readerException = new AtomicReference<>(null);
    private volatile boolean isClosed = false;
    private int consecutiveTimeoutCount = 0;
    private long totalWaitTimeMs = 0;
    private static final int MAX_CONSECUTIVE_TIMEOUTS = 5;
    private static final long TIMEOUT_INTERVAL_MS = 100;

    public HadoopHudiJniScanner(int fetchSize, Map<String, String> params) {
        this.basePath = params.get("base_path");
        this.dataFilePath = params.get("data_file_path");
        this.dataFileLength = Long.parseLong(params.get("data_file_length"));
        if (Strings.isNullOrEmpty(params.get("delta_file_paths"))) {
            this.deltaFilePaths = new String[0];
        } else {
            this.deltaFilePaths = params.get("delta_file_paths").split(",");
        }
        this.instantTime = params.get("instant_time");
        this.serde = params.get("serde");
        this.inputFormat = params.get("input_format");

        this.hudiColumnNames = params.get("hudi_column_names");
        this.hudiColumnTypes = params.get("hudi_column_types").split("#");
        if (StringUtils.isEmpty(params.get("required_fields"))) {
            this.requiredFields = params.get("hudi_primary_keys").split(",");
        } else {
            this.requiredFields = params.get("required_fields").split(",");
        }
        this.fieldInspectors = new ObjectInspector[requiredFields.length];
        this.structFields = new StructField[requiredFields.length];
        this.fsOptionsProps = Maps.newHashMap();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (entry.getKey().startsWith(HADOOP_CONF_PREFIX)) {
                fsOptionsProps.put(entry.getKey().substring(HADOOP_CONF_PREFIX.length()), entry.getValue());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("get hudi params {}: {}", entry.getKey(), entry.getValue());
            }
        }

        ZoneId zoneId;
        if (Strings.isNullOrEmpty(params.get("time_zone"))) {
            zoneId = ZoneId.systemDefault();
        } else {
            zoneId = ZoneId.of(params.get("time_zone"));
        }
        this.columnValue = new HadoopHudiColumnValue(zoneId);
        this.fetchSize = fetchSize;
        this.classLoader = this.getClass().getClassLoader();
        this.hadoopUserName = params.get("HADOOP_USER_NAME");
        this.hadoopUserToken = params.get("HADOOP_USER_TOKEN");

        // Initialize data queue with reasonable capacity
        this.dataQueue = new LinkedBlockingQueue<>(fetchSize * 2);
    }

    public String getBasePath() {
        return basePath;
    }

    public String getDataFilePath() {
        return dataFilePath;
    }

    public long getDataFileLength() {
        return dataFileLength;
    }

    public String[] getDeltaFilePaths() {
        return deltaFilePaths;
    }

    public String getInstantTime() {
        return instantTime;
    }

    public String getSerde() {
        return serde;
    }

    public String getInputFormat() {
        return inputFormat;
    }

    public String getHudiColumnNames() {
        return hudiColumnNames;
    }

    public String[] getHudiColumnTypes() {
        return hudiColumnTypes;
    }

    public String[] getRequiredFields() {
        return requiredFields;
    }

    public Map<String, String> getFsOptionsProps() {
        return fsOptionsProps;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public HadoopHudiColumnValue getColumnValue() {
        return columnValue;
    }

    public String getHadoopUserName() {
        return hadoopUserName;
    }

    public String getHadoopUserToken() {
        return hadoopUserToken;
    }

    @Override
    public void open() throws IOException {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            initRequiredColumnsAndTypes();
            initTableInfo(requiredTypes, requiredFields, fetchSize);
            Properties properties = getReaderProperties();
            initReader(properties);
        } catch (Exception e) {
            close();
            LOG.warn("failed to open hadoop hudi jni scanner", e);
            throw new IOException("failed to open hadoop hudi jni scanner: " + e.getMessage(), e);
        }
    }

    @Override
    public int getNext() throws IOException {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            // Check if there's an exception from background reader
            Exception exception = readerException.get();
            if (exception != null) {
                close();
                throw new IOException("Background reader error: " + exception.getMessage(), exception);
            }

            int numRows = 0;
            for (; numRows < fetchSize; numRows++) {
                RowData rowData;
                try {
                    // Try to get data from queue with timeout to avoid hanging forever
                    rowData = dataQueue.poll(TIMEOUT_INTERVAL_MS, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for data", e);
                }

                if (rowData == null) {
                    // No data available, check if reader is finished
                    if (isReaderFinished.get()) {
                        break; // No more data
                    }

                    // Increment consecutive timeout count and total wait time
                    consecutiveTimeoutCount++;
                    totalWaitTimeMs += TIMEOUT_INTERVAL_MS;
                    LOG.debug("Timeout waiting for data, consecutive timeout count: {}, total wait time: {}ms", 
                            consecutiveTimeoutCount, totalWaitTimeMs);

                    // Check if we've exceeded the maximum consecutive timeouts
                    if (consecutiveTimeoutCount >= MAX_CONSECUTIVE_TIMEOUTS) {
                        if (numRows > 0) {
                            break;
                        } else {
                            return -1;
                        }
                    }

                    // Reader is still active but no data yet, continue waiting
                    numRows--; // Don't count this iteration
                    continue;
                }

                // Reset consecutive timeout count and total wait time when we successfully get data
                consecutiveTimeoutCount = 0;
                totalWaitTimeMs = 0;

                // Process the row data
                Object rowObj = deserializer.deserialize(rowData.getValue());
                for (int i = 0; i < fields.length; i++) {
                    Object fieldData = rowInspector.getStructFieldData(rowObj, structFields[i]);
                    columnValue.setRow(fieldData);
                    columnValue.setField(types[i], fieldInspectors[i]);
                    appendData(i, columnValue);
                }
            }
            return numRows;
        } catch (Exception e) {
            close();
            LOG.warn("failed to get next in hadoop hudi jni scanner", e);
            throw new IOException("failed to get next in hadoop hudi jni scanner: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
            // Mark as closed to signal background task to stop
            isClosed = true;

            // Cancel background task if it's still running
            if (backgroundReaderTask != null && !backgroundReaderTask.isDone()) {
                backgroundReaderTask.cancel(true);
                try {
                    // Wait for background task to finish with timeout
                    backgroundReaderTask.get(5000, TimeUnit.MILLISECONDS);
                } catch (java.util.concurrent.TimeoutException e) {
                    LOG.warn("Timeout waiting for background reader task to finish");
                } catch (java.util.concurrent.ExecutionException e) {
                    LOG.warn("Background reader task execution failed", e.getCause());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted while waiting for background reader task to finish");
                }
            }

            // Close the reader
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            LOG.warn("failed to close hadoop hudi jni scanner", e);
            throw new IOException("failed to close hadoop hudi jni scanner: " + e.getMessage(), e);
        }
    }

    private void initRequiredColumnsAndTypes() {
        String[] splitHudiColumnNames = hudiColumnNames.split(",");

        Map<String, Integer> hudiColNameToIdx =
                IntStream.range(0, splitHudiColumnNames.length)
                        .boxed()
                        .collect(Collectors.toMap(i -> splitHudiColumnNames[i], i -> i));

        Map<String, String> hudiColNameToType =
                IntStream.range(0, splitHudiColumnNames.length)
                        .boxed()
                        .collect(Collectors.toMap(i -> splitHudiColumnNames[i], i -> hudiColumnTypes[i]));

        requiredTypes = Arrays.stream(requiredFields)
                .map(field -> ColumnType.parseType(field, hudiColNameToType.get(field)))
                .toArray(ColumnType[]::new);

        requiredColumnIds = Arrays.stream(requiredFields)
                .mapToInt(hudiColNameToIdx::get)
                .toArray();
    }

    private Properties getReaderProperties() {
        Properties properties = new Properties();
        properties.setProperty("hive.io.file.readcolumn.ids",
                Arrays.stream(this.requiredColumnIds).mapToObj(String::valueOf)
                        .collect(Collectors.joining(",")));
        properties.setProperty("hive.io.file.readcolumn.names", Joiner.on(",").join(this.requiredFields));
        properties.setProperty("columns", this.hudiColumnNames);
        properties.setProperty("columns.types", Joiner.on(",").join(hudiColumnTypes));
        properties.setProperty("serialization.lib", this.serde);
        properties.setProperty("hive.io.file.read.all.columns", "false");
        fsOptionsProps.forEach(properties::setProperty);
        return properties;
    }

    private void initReader(Properties properties) throws Exception {
        String realtimePath = dataFileLength != -1 ? dataFilePath : deltaFilePaths[0];
        long realtimeLength = dataFileLength != -1 ? dataFileLength : 0;
        Path path = new Path(realtimePath);
        FileSplit fileSplit = new FileSplit(path, 0, realtimeLength, (String[]) null);
        List<HoodieLogFile> logFiles = Arrays.stream(deltaFilePaths).map(HoodieLogFile::new)
                .collect(Collectors.toList());
        FileSplit hudiSplit =
                new HoodieRealtimeFileSplit(fileSplit, basePath, logFiles, instantTime, false, Option.empty());

        JobConf jobConf = new JobConf(new Configuration());
        properties.stringPropertyNames().forEach(name -> jobConf.set(name, properties.getProperty(name)));
        InputFormat<?, ?> inputFormatClass = getInputFormat(jobConf, inputFormat);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(hadoopUserName,
                null, hadoopUserToken);
        reader = ugi.doAs(
                (PrivilegedAction<RecordReader<NullWritable, ArrayWritable>>) () -> {
                    try {
                        return (RecordReader<NullWritable, ArrayWritable>) inputFormatClass.getRecordReader(
                                hudiSplit, jobConf, Reporter.NULL);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        deserializer = initDeserializer(jobConf, properties);
        rowInspector = initRowObjectInspector();
        for (int i = 0; i < requiredFields.length; i++) {
            StructField field = rowInspector.getStructFieldRef(requiredFields[i]);
            structFields[i] = field;
            fieldInspectors[i] = field.getFieldObjectInspector();
        }

        // Start background reader thread
        startBackgroundReader();
    }

    private InputFormat<?, ?> getInputFormat(Configuration conf, String inputFormat) throws Exception {
        Class<?> clazz = conf.getClassByName(inputFormat);
        Class<? extends InputFormat<?, ?>> cls =
                (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
        return ReflectionUtils.newInstance(cls, conf);
    }

    private Deserializer initDeserializer(Configuration configuration, Properties properties)
            throws Exception {
        Class<? extends Deserializer> deserializerClass = Class.forName(serde, true, JavaUtils.getClassLoader())
                .asSubclass(Deserializer.class);
        Deserializer deserializer = deserializerClass.getConstructor().newInstance();
        deserializer.initialize(configuration, properties);
        return deserializer;
    }

    private StructObjectInspector initRowObjectInspector() throws Exception {
        Preconditions.checkNotNull(deserializer);
        ObjectInspector inspector = deserializer.getObjectInspector();
        return (StructObjectInspector) inspector;
    }

    /**
     * Start background task using thread pool to read data from reader.next() and put into queue
     */
    private void startBackgroundReader() {
        backgroundReaderTask = BACKGROUND_READER_POOL.submit(() -> {
            try (ThreadClassLoaderContext ignored = new ThreadClassLoaderContext(classLoader)) {
                LOG.debug("Background reader task started");
                NullWritable key = reader.createKey();
                ArrayWritable value = reader.createValue();

                while (!isClosed && !Thread.currentThread().isInterrupted()) {
                    try {
                        boolean hasNext = reader.next(key, value);
                        if (!hasNext) {
                            LOG.debug("No more data available, reader finished");
                            isReaderFinished.set(true);
                            break;
                        }

                        // Create a copy of the data since ArrayWritable might be reused
                        ArrayWritable valueCopy = new ArrayWritable(value.get());
                        RowData rowData = new RowData(key, valueCopy);

                        // Put data into queue, this will block if queue is full
                        dataQueue.put(rowData);

                    } catch (InterruptedException e) {
                        LOG.debug("Background reader task interrupted");
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        LOG.warn("Error in background reader task", e);
                        readerException.set(e);
                        break;
                    }
                }
            } catch (Exception e) {
                LOG.warn("Fatal error in background reader task", e);
                readerException.set(e);
            } finally {
                LOG.debug("Background reader task finished");
                isReaderFinished.set(true);
            }
        });
    }
}
