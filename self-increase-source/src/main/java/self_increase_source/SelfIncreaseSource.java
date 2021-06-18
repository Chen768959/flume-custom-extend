package self_increase_source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;

/**
 * @author Chen768959
 * @date 2021/6/18
 */
public class SelfIncreaseSource extends AbstractSource implements
        PollableSource, Configurable, BatchSizeSupported {

  private int batchSize;

  @Override
  public Status process() throws EventDeliveryException {
    return null;
  }

  @Override
  public void configure(Context context) {

  }

  @Override
  public long getBatchSize() {
    return batchSize;
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
