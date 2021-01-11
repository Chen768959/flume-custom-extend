package pers.cly.merge_dfsfile_name_sink;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author Chen768959
 * @date 2021/1/10
 */
public class MergeDfsFileNameSink  extends AbstractSink implements Configurable {
  private String hdfsPath = "";
  private String fileUrl = "";
  private String hdfsUrl = "";

  private Configuration conf = new Configuration();

  private FileSystem hdfs;

  public Status process() throws EventDeliveryException {
    Status status = null;

    Channel ch = getChannel();
    Transaction txn = ch.getTransaction();

    //1.事务开始
    txn.begin();
    //2.开始将event存入hdfs
    try
    {
      Event event = ch.take();

      if (event != null)
      {
        System.out.println("获取到event");
        //匹配文件名
        String[] strings = event.getHeaders().get(fileUrl).split("/");
        String fileName = strings[strings.length-1];

        //以年月日作为子目录名并拼接文件名
        String hdfsPathAll = hdfsPath+nowData()+"/"+fileName;

        String body = new String(event.getBody(), "UTF-8");

        Path filePath = new Path(hdfsPathAll);

        // FileSystem hdfs = filePath.getFileSystem(conf);
        if (!hdfs.exists(filePath)) {
          hdfs.createNewFile(filePath);
        }
        FSDataOutputStream outputStream = hdfs.append(new Path(hdfsPathAll));
        outputStream.write(body.getBytes("UTF-8"));
        outputStream.write("\r\n".getBytes("UTF-8"));
        outputStream.flush();
        outputStream.close();
        hdfs.close();

        status = Status.READY;
      }
      else
      {
        status = Status.BACKOFF;
      }

      //3.整个过程走到这，表示hdfs提交成功，清除takelist中的数据，需要注意的是，此commit一定要紧跟着提交完毕的操作，
      // 否则明明已经提交完毕了，但是后面如果没有即使commit导致报错，则会进行回滚，把本次已提交的event又回退给channel，导致该event再一次提交。
      txn.commit();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      //4.出现意外，对事务进行回滚，即将takelist中的数据存回channel。
      txn.rollback();
      status = Status.BACKOFF;
    } catch (IOException e) {
      e.printStackTrace();
      txn.rollback();
      status = Status.BACKOFF;
    } finally {
      txn.close();
    }

    return status;
  }

  /**
   * 从conf文件获取定义好的常量
   * @param context
   * @author Chen768959
   * @date 2019/4/23 18:40
   * @return void
   */
  public void configure(Context context) {
    hdfsPath = context.getString("hdfspath");
    fileUrl = context.getString("fileUrl");
    hdfsUrl = context.getString("hdfsUrl");

    //hdfs路径
    conf.set("fs.defaultFS", hdfsUrl);

    //允许文件追加内容
    conf.setBoolean("dfs.support.append",true);

    /**
     * 设置为true时：
     * 如果在pipeline中存在一个DataNode故障时，
     * 那么hdfs客户端会从pipeline删除失败的DataNode，
     * 然后继续尝试往剩下的DataNodes进行写入。
     */
    conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);

    /**
     * 该属性只有在dfs.client.block.write.replace-datanode-on-failure.enable属性被设置为true时才有效。
     * 该属性可选值有三个：ALWAYS，NEVER，DEFAULT。
     * 用来控制当DataNode因为不能用被删除后，是否要往pipeline中添加新的DataNode，添加多少。
     * ALWAYS为只要删掉一个就新添加一个。
     * NEVER为永远不添加新的DataNode。
     * 设置该属性时要注意：当集群过小时，可能没有多余的DataNode可供添加，当设置了该属性但又没有节点可添加后会报错。
     */
    conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");

    try {
      hdfs = FileSystem.get(new URI(hdfsUrl),conf,"root");
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

  @Override
  public synchronized void stop()
  {
    super.stop();
  }

  @Override
  public synchronized void start()
  {
    try
    {
      super.start();
      System.out.println("finish start");
    }
    catch (Exception ex)
    {
      ex.printStackTrace();
    }
  }

  private String nowData(){
    Date d = new Date();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    String dateNowStr = sdf.format(d);
    return dateNowStr;
  }
}