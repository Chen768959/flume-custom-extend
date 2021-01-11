/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package pers.cly.tail_dir_include_child_source;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.stream.JsonReader;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.client.avro.ReliableEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReliableTaildirEventReader implements ReliableEventReader {
  private static final Logger logger = LoggerFactory.getLogger(ReliableTaildirEventReader.class);

  private final List<TailMatcher> taildirCache;
  private final Table<String, String, String> headerTable;

  private TailFile currentFile = null;
  private Map<Long, TailFile> tailFiles = Maps.newHashMap();
  private long updateTime;
  private boolean addByteOffset;
  private boolean cachePatternMatching;
  private boolean committed = true;
  private final boolean annotateFileName;
  private final String fileNameHeader;

  /**
   * Create a ReliableTaildirEventReader to watch the given directory.
   */
  private ReliableTaildirEventReader(Map<String, String> filePaths,
      Map<String, String> filePathsIncludeChild,
      Table<String, String, String> headerTable, String positionFilePath,
      boolean skipToEnd, boolean addByteOffset, boolean cachePatternMatching,
      boolean annotateFileName, String fileNameHeader) throws IOException {
    // Sanity checks
    if (filePaths == null && filePathsIncludeChild == null) {
      throw new NullPointerException();
    }
    Preconditions.checkNotNull(positionFilePath);

    if (logger.isDebugEnabled()) {
      logger.debug("Initializing {} with directory={}",
              new Object[]{ReliableTaildirEventReader.class.getSimpleName(), filePaths});
    }

    //taildirCache中保存了每个filegroup的真实路径，和其对应的文件正则规则
    List<TailMatcher> taildirCache = Lists.newArrayList();

    /**
     * 每一个TaildirMatcher，与一个配置文件中的一个group对应，
     * 它包含了group对应的目录与正则，其作用是根据正则与路径，找到目录下所有符合条件的文件
     * 且包含缓存机制。
     *
     * 此处根据配置文件中的group数，创建对应对象
     */
    if (filePaths != null) {
      for (Entry<String, String> e : filePaths.entrySet()) {
        taildirCache.add(new TaildirMatcher(e.getKey(), e.getValue(), cachePatternMatching));
      }
    }
    if (filePathsIncludeChild != null) {
      for (Entry<String, String> e : filePathsIncludeChild.entrySet()) {
        taildirCache.add(
                new TaildirIncludeChildMatcher(e.getKey(), e.getValue(), cachePatternMatching));
      }
    }

    logger.info("taildirCache: " + taildirCache.toString());
    logger.info("headerTable: " + headerTable.toString());

    this.taildirCache = taildirCache;
    this.headerTable = headerTable;
    this.addByteOffset = addByteOffset;
    this.cachePatternMatching = cachePatternMatching;
    this.annotateFileName = annotateFileName;
    this.fileNameHeader = fileNameHeader;

    /**
     * 检查filegroup目录里的文件是否有变动，如果有，则更新tailFiles列表
     * tailFiles列表中装了所有监控的文件信息
     *
     * 在第一次加载，也就是本次，会将每个filegroup中，符合正则的所有文件都加载进tailFiles
     */
    updateTailFiles(skipToEnd);

    logger.info("Updating position from position file: " + positionFilePath);
    loadPositionFile(positionFilePath);

    /**
     * 所以此构造方法一共做了以下几件事
     * 1、根据配置文件，将每个filegroup路径和其正则表达式封装起来，并将所有封装结果装入taildirCache
     * 2、根据taildirCache，将每个filegroup中，符合正则的所有文件都加载进tailFiles
     * 3、根据“读取位置记录文件”，将tailFiles中所有文件的“上一次读取位置”更新进每个文件，然后更新tailFiles。
     * 此时tailFiles中包含了所有待监控的文件，且每个里面都有自己的上次读取位置。
     */
  }

  /**
   * Load a position file which has the last read position of each file.
   * If the position file exists, update tailFiles mapping.
   * 加载每个文件的最后读取位置（注意，这个加载的是“位置文件”中的每个文件）
   */
  public void loadPositionFile(String filePath) {
    Long inode, pos;
    String path;
    FileReader fr = null;
    JsonReader jr = null;
    try {
      fr = new FileReader(filePath);
      //positionFilePath为json格式
      jr = new JsonReader(fr);
      jr.beginArray();
      while (jr.hasNext()) {
        inode = null;
        pos = null;
        path = null;
        jr.beginObject();
        while (jr.hasNext()) {
          switch (jr.nextName()) {
            case "inode":
              inode = jr.nextLong();
              break;
            case "pos":
              pos = jr.nextLong();
              break;
            case "file":
              path = jr.nextString();
              break;
          }
        }
        jr.endObject();

        for (Object v : Arrays.asList(inode, pos, path)) {
          Preconditions.checkNotNull(v, "Detected missing value in position file. "
              + "inode: " + inode + ", pos: " + pos + ", path: " + path);
        }
        TailFile tf = tailFiles.get(inode);
        //更新每个文件的最后读取pos位置
        if (tf != null && tf.updatePos(path, inode, pos)) {
          tailFiles.put(inode, tf);
        } else {
          logger.info("Missing file: " + path + ", inode: " + inode + ", pos: " + pos);
        }
      }
      jr.endArray();
    } catch (FileNotFoundException e) {
      logger.info("File not found: " + filePath + ", not updating position");
    } catch (IOException e) {
      logger.error("Failed loading positionFile: " + filePath, e);
    } finally {
      try {
        if (fr != null) fr.close();
        if (jr != null) jr.close();
      } catch (IOException e) {
        logger.error("Error: " + e.getMessage(), e);
      }
    }
  }

  public Map<Long, TailFile> getTailFiles() {
    return tailFiles;
  }

  public void setCurrentFile(TailFile currentFile) {
    this.currentFile = currentFile;
  }

  @Override
  public Event readEvent() throws IOException {
    List<Event> events = readEvents(1);
    if (events.isEmpty()) {
      return null;
    }
    return events.get(0);
  }

  @Override
  public List<Event> readEvents(int numEvents) throws IOException {
    return readEvents(numEvents, false);
  }

  @VisibleForTesting
  public List<Event> readEvents(TailFile tf, int numEvents) throws IOException {
    setCurrentFile(tf);
    return readEvents(numEvents, true);
  }

  public List<Event> readEvents(int numEvents, boolean backoffWithoutNL)
      throws IOException {
    /**
     * committed默认值为true，所以一开始不会走到这，
     * 当需要提交，但是由于channel满了等原因没提交成功时，会走到这个地方，
     * 如果有未提交的任务,代表之前的任务失败了,进行回滚操作.即将上一次
     */
    if (!committed) {
      if (currentFile == null) {
        throw new IllegalStateException("current file does not exist. ");
      }
      logger.info("Last read was never committed - resetting position");
      long lastPos = currentFile.getPos();
      currentFile.updateFilePos(lastPos);
    }
    //从当前文件中读取“每行（按字节读取，读到换行符则返回byte数组）”，将此次读取的初始文件位置存入event的header中
    List<Event> events = currentFile.readEvents(numEvents, backoffWithoutNL, addByteOffset);
    if (events.isEmpty()) {
      return events;
    }

    //读取配置文件中headers.参数，将headerKey加入event的header中
    Map<String, String> headers = currentFile.getHeaders();
    if (annotateFileName || (headers != null && !headers.isEmpty())) {
      for (Event event : events) {
        if (headers != null && !headers.isEmpty()) {
          event.getHeaders().putAll(headers);
        }
        if (annotateFileName) {
          event.getHeaders().put(fileNameHeader, currentFile.getPath());
        }
      }
    }
    committed = false;
    return events;
  }

  @Override
  public void close() throws IOException {
    for (TailFile tf : tailFiles.values()) {
      if (tf.getRaf() != null) tf.getRaf().close();
    }
  }

  /** Commit the last lines which were read. */
  @Override
  public void commit() throws IOException {
    if (!committed && currentFile != null) {
      long pos = currentFile.getLineReadPos();
      currentFile.setPos(pos);
      currentFile.setLastUpdated(updateTime);
      committed = true;
    }
  }

  /**
   * Update tailFiles mapping if a new file is created or appends are detected
   * to the existing file.
   * 判断filegroup目录下的文件是否有更新，如果有更新则将文件的状态值设置为"已更新"，并返回所有文件的inode号码。
   * 注意，所有的filegroup是从缓存变量taildirCache中获取的
   * 在初始化时会，第一次调用该方法。
   * 运行过程中会不断定时调用该方法。
   */
  public List<Long> updateTailFiles(boolean skipToEnd) throws IOException {
    updateTime = System.currentTimeMillis();
    List<Long> updatedInodes = Lists.newArrayList();

    /**
     * taildirCache中装的是每个filegroup的根路径，和其正则规则（封装进TaildirMatcher对象）
     * 此步骤实际上是迭代每个filegroup路径
     */
    for (TailMatcher taildir : taildirCache) {
      Map<String, String> headers = headerTable.row(taildir.getFileGroup());

      for (File f : taildir.getMatchingFiles()) {
        long inode;
        try {
          //获取文件inode号码（linux系统通过inode号码来识别文件）
          inode = getInode(f);
        } catch (NoSuchFileException e) {
          taildir.deleteFileCache(f);
          logger.info("File has been deleted in the meantime: " + e.getMessage());
          continue;
        }
        /**
         * 在start方法时，tailFiles已经第一次装满了所有正则出来的文件对象。
         * 所以只要不是新建的文件inode，都能在tailFiles中找到tf，
         * 如果是新建文件，则将pos读取位置设为0（即该文件的初始位置）。
         * 如果是旧文件，则判断文件是否被修改，如果被修改，则更新该文件的修改标示为“被修改”。
         *
         * 不管是不是新建，或有没有修改，都会被重新更新进tailFiles，并将其inode返回。
         *
         * 也就是说，每调用一次该方法，都会重新扫描一遍各filegroup路径下符合规则的文件，并返回出去
         */
        TailFile tf = tailFiles.get(inode);
        if (tf == null || !tf.getPath().equals(f.getAbsolutePath())) {
          long startPos = skipToEnd ? f.length() : 0;
          tf = openFile(f, headers, inode, startPos);
        } else {
          //getLastUpdated方法会随着后续process不断更新，tf.getLastUpdated是文件上一次被监控的时间
          //lastModified为文件最后一次被修改的时间
          //如果文件的修改时间大于上一次被监控的时间，自然文件就更新了，也就需要监控其更新部分
          boolean updated = tf.getLastUpdated() < f.lastModified() || tf.getPos() != f.length();
          //判断是否监控其更新部分
          if (updated) {
            if (tf.getRaf() == null) {
              tf = openFile(f, headers, inode, tf.getPos());
            }
            if (f.length() < tf.getPos()) {
              logger.info("Pos " + tf.getPos() + " is larger than file size! "
                  + "Restarting from pos 0, file: " + tf.getPath() + ", inode: " + inode);
              tf.updatePos(tf.getPath(), inode, 0);
            }
          }
          tf.setNeedTail(updated);
        }
        //更新tailFiles
        tailFiles.put(inode, tf);
        updatedInodes.add(inode);
      }
    }
    return updatedInodes;
  }

  public List<Long> updateTailFiles() throws IOException {
    return updateTailFiles(false);
  }


  private long getInode(File file) throws IOException {
    long inode = (long) Files.getAttribute(file.toPath(), "unix:ino");
    return inode;
  }

  private TailFile openFile(File file, Map<String, String> headers, long inode, long pos) {
    try {
      logger.info("Opening file: " + file + ", inode: " + inode + ", pos: " + pos);
      return new TailFile(file, headers, inode, pos);
    } catch (IOException e) {
      throw new FlumeException("Failed opening file: " + file, e);
    }
  }

  /**
   * Special builder class for ReliableTaildirEventReader
   */
  public static class Builder {
    private Map<String, String> filePaths;
    private Map<String, String> filePathsIncludeChild;
    private Table<String, String, String> headerTable;
    private String positionFilePath;
    private boolean skipToEnd;
    private boolean addByteOffset;
    private boolean cachePatternMatching;
    private Boolean annotateFileName =
            TaildirSourceConfigurationConstants.DEFAULT_FILE_HEADER;
    private String fileNameHeader =
            TaildirSourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;

    public Builder filePaths(Map<String, String> filePaths) {
      this.filePaths = filePaths;
      return this;
    }

    public Builder filePathsIncludeChild(Map<String, String> filePathsIncludeChild) {
      this.filePathsIncludeChild = filePathsIncludeChild;
      return this;
    }

    public Builder headerTable(Table<String, String, String> headerTable) {
      this.headerTable = headerTable;
      return this;
    }

    public Builder positionFilePath(String positionFilePath) {
      this.positionFilePath = positionFilePath;
      return this;
    }

    public Builder skipToEnd(boolean skipToEnd) {
      this.skipToEnd = skipToEnd;
      return this;
    }

    public Builder addByteOffset(boolean addByteOffset) {
      this.addByteOffset = addByteOffset;
      return this;
    }

    public Builder cachePatternMatching(boolean cachePatternMatching) {
      this.cachePatternMatching = cachePatternMatching;
      return this;
    }

    public Builder annotateFileName(boolean annotateFileName) {
      this.annotateFileName = annotateFileName;
      return this;
    }

    public Builder fileNameHeader(String fileNameHeader) {
      this.fileNameHeader = fileNameHeader;
      return this;
    }

    public ReliableTaildirEventReader build() throws IOException {
      return new ReliableTaildirEventReader(filePaths, filePathsIncludeChild, headerTable,
                                            positionFilePath, skipToEnd,
                                            addByteOffset, cachePatternMatching,
                                            annotateFileName, fileNameHeader);
    }
  }

}
