package self_increase_source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Chen768959
 * @date 2021/6/18
 */
public class SelfIncreaseSource extends AbstractSource implements Configurable, PollableSource  {
  private static final Logger logger = LoggerFactory.getLogger(SelfIncreaseSource.class);

  private int batchSize;

  private int intervalMs;

  private IncreaseFactory increaseFactory;

  @Override
  public Status process() throws EventDeliveryException {
    Status status = null;

    try {
      List<Event> events = increaseFactory.increaseEvents(batchSize);

      // 获取ChannelProcessor对象，将events列表交给他
      getChannelProcessor().processEventBatch(events);

      status = Status.READY;
    } catch (Throwable t) {
      logger.error("生成异常",t);
      status = Status.BACKOFF;
      // re-throw all Errors
      if (t instanceof Error) {
        throw (Error)t;
      }
    }

    if (intervalMs>0){
      try {
        Thread.sleep(intervalMs);
      } catch (InterruptedException e) {
        logger.error("间隔异常：",e);
      }
    }
    return status;
  }

  @Override
  public void configure(Context context) {
    batchSize = context.getInteger("batch-size", 100);

    // 每次处理完后时间
    intervalMs = context.getInteger("interval-ms", 2000);

    increaseFactory = new IncreaseFactoryDef();
  }

  @Override
  public long getBackOffSleepIncrement() {
    return 1000L;
  }

  @Override
  public long getMaxBackOffSleepInterval() {
    return 5000L;
  }
}
