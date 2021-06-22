package self_increase_source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import self_increase_source.data.AppData1;
import self_increase_source.data.AppData2;
import self_increase_source.data.AppData3;
import self_increase_source.data.AppData4;
import self_increase_source.data.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @author Chen768959
 * @date 2021/6/21
 */
public class IncreaseFactoryAppDataDef implements IncreaseFactory{
  private static final Logger logger = LoggerFactory.getLogger(IncreaseFactoryAppDataDef.class);

  private final ObjectMapper objectMapper = new ObjectMapper();

  private static final Random random = new Random();

  // 总数据
  private Data appData1;
  private Data appData2;
  private Data appData3;
  private Data appData4;

  public IncreaseFactoryAppDataDef(){
    this.appData1 = new AppData1();
    this.appData2 = new AppData2();
    this.appData3 = new AppData3();
    this.appData4 = new AppData4();
  }

  @Override
  public List<Event> increaseEvents(int increaseNum) throws JsonProcessingException {
    List<Event> eventList = new ArrayList<>(increaseNum);

    Data data = appData1;
    for (int i=0; i<increaseNum; i++){
      Event event = new SimpleEvent();

      switch (random.nextInt(4)){
        case 0:
          data = appData1;
          break;
        case 1:
          data = appData2;
          break;
        case 2:
          data = appData3;
          break;
        case 3:
          data = appData4;
          break;
      }

      data.makingRandom();
      event.setBody(getJsonData(data.getData()));

      eventList.add(event);
    }

    return eventList;
  }

  private byte[] getJsonData(Map<String,Object> map) throws JsonProcessingException {
    return objectMapper.writeValueAsBytes(map);
  }
}
