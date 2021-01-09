package pers.cly.merge_file_name_sink;

import com.google.common.collect.Lists;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Chen768959
 * @date 2019/4/23
 */
public class MergeFileNameSink extends AbstractSink implements Configurable {
  //本地目标路径
  private String targetUrl = "";
  //原始文件路径，需要正则出文件名
  private String fileUrl = "";
  //sink一个事务中，从queue队列中取出处理的event数量。（默认100）
  private int batchSize = 100;

  private static final Logger LOG = LoggerFactory.getLogger(MergeFileNameSink.class);


  public Status process() throws EventDeliveryException {
    Status status = Status.READY;

    //获取Channel对象
    Channel channel = getChannel();
    //获取事务对象
    Transaction transaction = channel.getTransaction();

    transaction.begin();

    try {
      //一次性处理batch-size个event对象
      List<Event> batch = Lists.newLinkedList();
      for (int i = 0; i < batchSize; i++) {
        Event event = channel.take();

        if (event == null) {
          break;
        }
        batch.add(event);
      }
      int size = batch.size();
      if (size == 0) {
        //BACKOFF表示让flume睡眠一段时间（因为此时已经取不出来event了）
        status =  Status.BACKOFF;
      } else {//process
        Map<String, List<Event>> relateFileEventsMap = new HashMap<>();
        String fileName = "";
        for (Event event : batch) {
          //获取event所属文件名
          String[] strings = event.getHeaders().get(fileUrl).split("/");
          fileName = strings[strings.length - 1];

          //判断map中是否有此文件所对应的event列表，如果没有则新建关系。
          List<Event> relateEvents = relateFileEventsMap.get(fileName);
          if (relateEvents == null) {
            List<Event> newRelateEvents = new ArrayList<>();
            newRelateEvents.add(event);
            relateFileEventsMap.put(fileName, newRelateEvents);
          } else {
            relateEvents.add(event);
          }
        }

        StringBuilder filePath = new StringBuilder().append(targetUrl).append("/").append(nowData()).append("/");
        //迭代所有文件和其event列表，将events写入其对应文件
        for (String relateFileName : relateFileEventsMap.keySet()) {

          //文件路径
          filePath.append(relateFileName);
          //如果文件不存在，则创建新的文件
          File file = new File(filePath.toString());
          File fileParent = file.getParentFile();
          if (!fileParent.exists()) {
            fileParent.mkdirs();
          }
          if (!file.exists()) {
            file.createNewFile();
          }

          //将该文件对应的events全部写入文件
          FileOutputStream fileOutputStream = new FileOutputStream(file, true);
          List<Event> events = relateFileEventsMap.get(relateFileName);
          for (Event event : events) {
            fileOutputStream.write(event.getBody());
            // 写入一个换行
            fileOutputStream.write("\r\n".getBytes());
          }

          fileOutputStream.close();
        }
        //READY表示event可以提交了
        status =  Status.READY;
      }

      //如果执行成功，最后一定要提交
      transaction.commit();
    } catch (IOException eIO) {
      transaction.rollback();
      LOG.warn("CustomLocalSink IO error", eIO);
      status =  Status.BACKOFF;
    } catch (Throwable th) {
      transaction.rollback();
      LOG.error("CustomLocalSink process failed", th);
      if (th instanceof Error) {
        throw (Error) th;
      } else {
        throw new EventDeliveryException(th);
      }
    }finally {
      //在关闭事务之前，必须执行过事务的提交或回退
      transaction.close();
    }

    //将状态返回出去
    return status;
  }

  /**
   * 从conf文件获取定义好的常量
   *
   * @param context
   * @return void
   * @author Chen768959
   * @date 2019/4/23 18:40
   */
  public void configure(Context context) {
    targetUrl = context.getString("targetUrl");
    fileUrl = context.getString("fileUrl");
    batchSize = context.getInteger("batchSize");
  }

  @Override
  public synchronized void stop() {
    super.stop();
  }


  @Override
  public synchronized void start() {
    try {
      super.start();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }


  private String nowData() {
    Date d = new Date();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    String dateNowStr = sdf.format(d);
    return dateNowStr;
  }
}
