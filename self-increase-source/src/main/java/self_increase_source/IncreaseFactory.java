package self_increase_source;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flume.Event;

import java.util.List;

/**
 * @author Chen768959
 * @date 2021/6/19
 */
public interface IncreaseFactory {
  /**
   * 自增Event方法
   * @param increaseNum 每次生成的数量
   * @author Chen768959
   * @date 2021/6/19 下午 7:04
   * @return java.util.List<org.apache.flume.Event>
   */
  List<Event> increaseEvents(int increaseNum) throws JsonProcessingException;
}
