package self_increase_source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import self_increase_source.data.Data;
import self_increase_source.data.WebData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Chen768959
 * @date 2021/6/21
 */
public class IncreaseFactoryWebDataDef implements IncreaseFactory{
  private static final Logger logger = LoggerFactory.getLogger(IncreaseFactoryWebDataDef.class);

  private final ObjectMapper objectMapper = new ObjectMapper();

  // 总数据
  private final Data webData;

  //生成测试数据主体
  public IncreaseFactoryWebDataDef(){
    this.webData = new WebData();
  }

  @Override
  public List<Event> increaseEvents(int increaseNum) throws JsonProcessingException {
    List<Event> eventList = new ArrayList<>(increaseNum);

    for (int i=0; i<increaseNum; i++){
      // 制造随机数
      webData.makingRandom();

      // 获取当前对象的json形态
      byte[] webDataJson = getJsonData(webData.getData());

      Event event = new SimpleEvent();
      event.setBody(webDataJson);

      eventList.add(event);
    }

    return eventList;
  }

  private byte[] getJsonData(Map<String,Object> map) throws JsonProcessingException {
    return objectMapper.writeValueAsBytes(map);
  }
}
