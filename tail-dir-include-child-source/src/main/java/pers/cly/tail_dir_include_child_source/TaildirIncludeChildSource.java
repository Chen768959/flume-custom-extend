/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package pers.cly.tail_dir_include_child_source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.PollableSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static pers.cly.tail_dir_include_child_source.TaildirSourceConfigurationConstants.*;

public class TaildirIncludeChildSource extends AbstractSource implements
    PollableSource, Configurable, BatchSizeSupported {

  private static final Logger logger = LoggerFactory.getLogger(TaildirIncludeChildSource.class);

  private Map<String, String> filePaths;
  private Map<String, String> filePathsIncludeChild;
  private Table<String, String, String> headerTable;
  private int batchSize;
  private String positionFilePath;
  private boolean skipToEnd;
  private boolean byteOffsetHeader;

  private SourceCounter sourceCounter;
  private ReliableTaildirEventReader reader;
  private ScheduledExecutorService idleFileChecker;
  private ScheduledExecutorService positionWriter;
  private int retryInterval = 1000;
  private int maxRetryInterval = 5000;
  private int idleTimeout;
  private int checkIdleInterval = 5000;
  private int writePosInitDelay = 5000;
  private int writePosInterval;
  private boolean cachePatternMatching;

  private List<Long> existingInodes = new CopyOnWriteArrayList<Long>();
  private List<Long> idleInodes = new CopyOnWriteArrayList<Long>();
  private Long backoffSleepIncrement;
  private Long maxBackOffSleepInterval;
  private boolean fileHeader;
  private String fileHeaderKey;
  private Long maxBatchCount;

  @Override
  public synchronized void start() {
    logger.info("{} TaildirSource source starting with directory: {}", getName(), filePaths);
    try {
      //将所有的配置信息加载，并创建ReliableTaildirEventReader类
      //正则出每个filegroup中所有待监控的文件，且每个文件都有自己的上次读取位置。
      reader = new ReliableTaildirEventReader.Builder()
          .filePaths(filePaths)
          .filePathsIncludeChild(filePathsIncludeChild)
          .headerTable(headerTable)
          .positionFilePath(positionFilePath)
          .skipToEnd(skipToEnd)
          .addByteOffset(byteOffsetHeader)
          .cachePatternMatching(cachePatternMatching)
          .annotateFileName(fileHeader)
          .fileNameHeader(fileHeaderKey)
          .build();
    } catch (IOException e) {
      throw new FlumeException("Error instantiating ReliableTaildirEventReader", e);
    }

    //定时检查所有TailFiles，将其中需要被关闭的文件提取出来
    idleFileChecker = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("idleFileChecker").build());
    idleFileChecker.scheduleWithFixedDelay(new idleFileCheckerRunnable(),
        idleTimeout, checkIdleInterval, TimeUnit.MILLISECONDS);

    /**
     创建线程池，开启定时任务，定时运行PositionWriterRunnable()
     获得existingInodes中的所有inode码，找到TailFiles中对应文件对象，将其pos位置等信息转化成json数据写入位置文件
     （当第一次运行flume时，existingInodes为空，后续process运行起来后，才会有existingInodes）
     */
    positionWriter = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("positionWriter").build());
    positionWriter.scheduleWithFixedDelay(new PositionWriterRunnable(),
        writePosInitDelay, writePosInterval, TimeUnit.MILLISECONDS);

    super.start();
    logger.debug("TaildirSource started");
    sourceCounter.start();
  }

  @Override
  public synchronized void stop() {
    try {
      super.stop();
      ExecutorService[] services = {idleFileChecker, positionWriter};
      for (ExecutorService service : services) {
        service.shutdown();
        if (!service.awaitTermination(1, TimeUnit.SECONDS)) {
          service.shutdownNow();
        }
      }
      // write the last position
      writePosition();
      reader.close();
    } catch (InterruptedException e) {
      logger.info("Interrupted while awaiting termination", e);
    } catch (IOException e) {
      logger.info("Failed: " + e.getMessage(), e);
    }
    sourceCounter.stop();
    logger.info("Taildir source {} stopped. Metrics: {}", getName(), sourceCounter);
  }

  @Override
  public String toString() {
    return String.format("Taildir source: { positionFile: %s, skipToEnd: %s, "
        + "byteOffsetHeader: %s, idleTimeout: %s, writePosInterval: %s }",
        positionFilePath, skipToEnd, byteOffsetHeader, idleTimeout, writePosInterval);
  }

  @Override
  public synchronized void configure(Context context) {
    //从配置文件中获取a1.sources.r1.filegroups，以空格分隔文件组列表，每个文件组都指示一组要挂起的文件
    String fileGroups = context.getString(FILE_GROUPS);
    String fileGroupsIncludeChild = context.getString(FILE_GROUPS_INCLUDE_CHILD);
    Preconditions.checkState(fileGroups != null ||
            fileGroupsIncludeChild != null, "Missing param: " + FILE_GROUPS);

    Map<String, String> filePathsMap = context.getSubProperties(FILE_GROUPS_PREFIX);
    if (!filePathsMap.isEmpty()) {
      filePaths = selectByKeys(filePathsMap,
              fileGroups.split("\\s+"));
      Preconditions.checkState(!filePaths.isEmpty(),
              "Mapping for tailing files is empty or invalid: '" + FILE_GROUPS_PREFIX + "'");
    }

    Map<String, String> filePathsIncludeChildMap =
            context.getSubProperties(FILE_GROUPS_INCLUDE_CHILD_PREFIX);
    if (!filePathsIncludeChildMap.isEmpty()) {
      filePathsIncludeChild = selectByKeys(filePathsIncludeChildMap,
              fileGroupsIncludeChild.split("\\s+"));
      Preconditions.checkState(!filePathsIncludeChild.isEmpty(),
              "Mapping for tailing files is empty or invalid: '" +
                      FILE_GROUPS_INCLUDE_CHILD_PREFIX + "'");
    }

    //获取当前用户主目录
    String homePath = System.getProperty("user.home").replace('\\', '/');

    //获取positionFile文件路径，没有则使用默认路径“~/.flume/taildir_position.json”
    //该文件里面记录了每个被读取的文件的偏移量
    positionFilePath = context.getString(POSITION_FILE, homePath + DEFAULT_POSITION_FILE);
    Path positionFile = Paths.get(positionFilePath);
    try {
      //创建positionFile的目录，上级目录如果缺失则一起创建
      Files.createDirectories(positionFile.getParent());
    } catch (IOException e) {
      throw new FlumeException("Error creating positionFile parent directories", e);
    }

    //用于发送EVENT的header信息添加值,对应配置文件中的headers
    headerTable = getTable(context, HEADERS_PREFIX);

    //batchSize大小，即每次处理的event数量，默认100
    batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);

    //是否从尾部读取，默认false
    skipToEnd = context.getBoolean(SKIP_TO_END, DEFAULT_SKIP_TO_END);

    //是否加偏移量，剔除行标题 默认 false
    byteOffsetHeader = context.getBoolean(BYTE_OFFSET_HEADER, DEFAULT_BYTE_OFFSET_HEADER);

    //文件在“idleTimeout”时间间隔内没有被修改，则关闭文件，默认120000毫秒
    idleTimeout = context.getInteger(IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT);

    //更新“位置文件positionFile（里面记录了每个被读取的文件的偏移量）”的间隔时间（毫秒）。
    writePosInterval = context.getInteger(WRITE_POS_INTERVAL, DEFAULT_WRITE_POS_INTERVAL);

    //是否开启matcher cache 默认: true
    cachePatternMatching = context.getBoolean(CACHE_PATTERN_MATCHING,
        DEFAULT_CACHE_PATTERN_MATCHING);

    //当Kafka topic为空时触发的初始和增量等待时间。等待期将减少空的kafka topic的通讯。一般来说，一秒钟是理想的。
    //当最后一次尝试没有找到任何新数据时，推迟变量长的时间再次轮训查找，默认推迟1秒
    backoffSleepIncrement = context.getLong(PollableSourceConstants.BACKOFF_SLEEP_INCREMENT,
        PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);

    //当最后一次尝试没有找到任何新数据时,每次重新尝试轮询新数据之间的最大时间延迟 . 默认值: 5秒
    maxBackOffSleepInterval = context.getLong(PollableSourceConstants.MAX_BACKOFF_SLEEP,
        PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);

    //是否添加头部存储绝对路径 默认: false
    fileHeader = context.getBoolean(FILENAME_HEADER,
            DEFAULT_FILE_HEADER);

    //当fileHeader为TURE时使用。  默认头文件信息 key : file
    fileHeaderKey = context.getString(FILENAME_HEADER_KEY,
            DEFAULT_FILENAME_HEADER_KEY);

    //最大批次数量，默认Long.MAX_VALUE  2^63-1
    maxBatchCount = context.getLong(MAX_BATCH_COUNT, DEFAULT_MAX_BATCH_COUNT);
    if (maxBatchCount <= 0) {
      maxBatchCount = DEFAULT_MAX_BATCH_COUNT;
      logger.warn("Invalid maxBatchCount specified, initializing source "
          + "default maxBatchCount of {}", maxBatchCount);
    }

    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
  }

  @Override
  public long getBatchSize() {
    return batchSize;
  }

  private Map<String, String> selectByKeys(Map<String, String> map, String[] keys) {
    Map<String, String> result = Maps.newHashMap();
    for (String key : keys) {
      if (map.containsKey(key)) {
        result.put(key, map.get(key));
      }
    }
    return result;
  }

  private Table<String, String, String> getTable(Context context, String prefix) {
    Table<String, String, String> table = HashBasedTable.create();
    for (Entry<String, String> e : context.getSubProperties(prefix).entrySet()) {
      String[] parts = e.getKey().split("\\.", 2);
      table.put(parts[0], parts[1], e.getValue());
    }
    return table;
  }

  @VisibleForTesting
  protected SourceCounter getSourceCounter() {
    return sourceCounter;
  }

  @Override
  public Status process() {
    Status status = Status.BACKOFF;
    try {
      /**
       * 每次运行都会更新existingInodes，start中的定时任务只会将existingInodes中文件对象的位置信息更新进位置文件中
       * 而后也是判断的existingInodes中是否有更新了的文件，将其更新增量封装成event传递出去。
       * 所以flume运行后，其循环调用的updateTailFiles()方法决定了每次检测的文件
       */
      existingInodes.clear();
      //通过reader对象，获取最新的待监控文件列表
      existingInodes.addAll(reader.updateTailFiles());
      //迭代文件列表
      for (long inode : existingInodes) {
        TailFile tf = reader.getTailFiles().get(inode);
        if (tf.needTail()) {
          //每次处理一个文件，获取该文件的events列表，并发送进channel
          boolean hasMoreLines = tailFileProcess(tf, true);
          if (hasMoreLines) {
            status = Status.READY;
          }
        }
      }
      closeTailFiles();
    } catch (Throwable t) {
      logger.error("Unable to tail files", t);
      sourceCounter.incrementEventReadFail();
      status = Status.BACKOFF;
    }
    return status;
  }

  @Override
  public long getBackOffSleepIncrement() {
    return backoffSleepIncrement;
  }

  @Override
  public long getMaxBackOffSleepInterval() {
    return maxBackOffSleepInterval;
  }

  /**
   * 从该文件中读取batchSize行（即batchSize个event）
   */
  private boolean tailFileProcess(TailFile tf, boolean backoffWithoutNL)
      throws IOException, InterruptedException {
    long batchCount = 0;
    while (true) {
      reader.setCurrentFile(tf);
      /**
       * batchSize是配置文件中定义的值，表示一次处理的event数量
       * 从当前文件中读取“每行”，并将每行转变为byte数组，
       * 将此次读取的文件位置存入每个event的header中。
       * 将配置文件中的headers参数也传入每个event的header中。
       */
      List<Event> events = reader.readEvents(batchSize, backoffWithoutNL);
      if (events.isEmpty()) {
        return false;
      }
      //到目前为止source已经接收到的event总数量
      sourceCounter.addToEventReceivedCount(events.size());
      sourceCounter.incrementAppendBatchReceivedCount();
      try {
        /**
         * 获取ChannelProcessor对象，将events列表交给他
         */
        getChannelProcessor().processEventBatch(events);
        /**
         * 当执行到这时，就表示events已经提交完毕
         * 更新文件的pos位置为最后读取的一行
         */
        reader.commit();
      } catch (ChannelException ex) {
        logger.warn("The channel is full or unexpected failure. " +
            "The source will try again after " + retryInterval + " ms");
        sourceCounter.incrementChannelWriteFail();
        TimeUnit.MILLISECONDS.sleep(retryInterval);
        retryInterval = retryInterval << 1;
        retryInterval = Math.min(retryInterval, maxRetryInterval);
        continue;
      }
      retryInterval = 1000;
      sourceCounter.addToEventAcceptedCount(events.size());
      sourceCounter.incrementAppendBatchAcceptedCount();
      if (events.size() < batchSize) {
        logger.debug("The events taken from " + tf.getPath() + " is less than " + batchSize);
        return false;
      }
      if (++batchCount >= maxBatchCount) {
        logger.debug("The batches read from the same file is larger than " + maxBatchCount );
        return true;
      }
    }
  }

  private void closeTailFiles() throws IOException, InterruptedException {
    for (long inode : idleInodes) {
      TailFile tf = reader.getTailFiles().get(inode);
      if (tf.getRaf() != null) { // when file has not closed yet
        tailFileProcess(tf, false);
        tf.close();
        logger.info("Closed file: " + tf.getPath() + ", inode: " + inode + ", pos: " + tf.getPos());
      }
    }
    idleInodes.clear();
  }

  /**
   * Runnable class that checks whether there are files which should be closed.
   * 检查是否应该关闭文件。
   * 迭代TailFiles中的每个文件对象，如果该文件的最后检测时间加检测间隔，小于当前时间，
   * 就将该文件inode存入idleInodes列表，process中会关闭这些文件
   */
  private class idleFileCheckerRunnable implements Runnable {
    @Override
    public void run() {
      try {
        long now = System.currentTimeMillis();
        /**
         * TailFiles中包含了所有待监控的文件，且每个里面都有自己的上次读取位置。
         * 迭代出每个tf，就是每个待监控文件
         */
        for (TailFile tf : reader.getTailFiles().values()) {
          /**
           * idleTimeout是每个文件的监控间隔时间
           */
          if (tf.getLastUpdated() + idleTimeout < now && tf.getRaf() != null) {
            idleInodes.add(tf.getInode());
          }
        }
      } catch (Throwable t) {
        logger.error("Uncaught exception in IdleFileChecker thread", t);
        sourceCounter.incrementGenericProcessingFail();
      }
    }
  }

  /**
   * Runnable class that writes a position file which has the last read position
   * of each file.
   */
  private class PositionWriterRunnable implements Runnable {
    @Override
    public void run() {
      writePosition();
    }
  }

  /**
   * existingInodes中储存了所有待检查文件inode号码
   */
  private void writePosition() {
    File file = new File(positionFilePath);
    FileWriter writer = null;
    try {
      //打开位置文件
      writer = new FileWriter(file);
      //当第一次运行flume时，existingInodes为空，后续process运行起来后，才会有existingInodes
      if (!existingInodes.isEmpty()) {
        //获得existingInodes中的所有inode码，找到TailFiles中对应文件对象，将其pos位置等信息转化成json数据写入位置文件
        String json = toPosInfoJson();
        writer.write(json);
      }
    } catch (Throwable t) {
      logger.error("Failed writing positionFile", t);
      sourceCounter.incrementGenericProcessingFail();
    } finally {
      try {
        if (writer != null) writer.close();
      } catch (IOException e) {
        logger.error("Error: " + e.getMessage(), e);
        sourceCounter.incrementGenericProcessingFail();
      }
    }
  }

  /**
   * 迭代所有待检查文件
   */
  private String toPosInfoJson() {
    @SuppressWarnings("rawtypes")
    List<Map> posInfos = Lists.newArrayList();
    for (Long inode : existingInodes) {
      TailFile tf = reader.getTailFiles().get(inode);
      posInfos.add(ImmutableMap.of("inode", inode, "pos", tf.getPos(), "file", tf.getPath()));
    }
    return new Gson().toJson(posInfos);
  }
}
