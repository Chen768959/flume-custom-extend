package self_increase_source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
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
  private final Map<String,Object> webData;

  //需要设置随机数的entry
  private Map.Entry<String, Object> entrySID;
  private Map.Entry<String, Object> entryWID;
  private Map.Entry<String, Object> entryEVENTS_TM;
  private Map.Entry<String, Object> entryEVENTS_EA_cookieId;
  private Map.Entry<String, Object> entryEVENTS_EA_playSessionId;
  private Map.Entry<String, Object> entryEVENTS_EA_program_id;
  private Map.Entry<String, Object> entryEVENTS_EA_contentId;

  //生成测试数据主体
  public IncreaseFactoryWebDataDef(){
    this.webData = getWebDataMain();

    // 获取需要设置随机数的entry
    this.entrySID = getEntryByKey(webData,"SID");
    this.entryWID = getEntryByKey(webData,"WID");

    Map<String, Object> eventsMap = getArrByKey(webData, "EVENTS").get(0);
    this.entryEVENTS_TM = getEntryByKey(eventsMap,"TM");

    ArrayList<Map<String, Object>> eaArr = getArrByKey(eventsMap, "EA");
    for (Map<String, Object> map : eaArr){
      if (map.get("EK").equals("cookieId")){
        this.entryEVENTS_EA_cookieId = getEntryByKey(map,"EV");
        continue;
      }
      if (map.get("EK").equals("playSessionId")){
        this.entryEVENTS_EA_playSessionId = getEntryByKey(map,"EV");
        continue;
      }
      if (map.get("EK").equals("program_id")){
        this.entryEVENTS_EA_program_id = getEntryByKey(map,"EV");
        continue;
      }
      if (map.get("EK").equals("contentId")){
        this.entryEVENTS_EA_contentId = getEntryByKey(map,"EV");
        continue;
      }
    }
  }

  @Override
  public List<Event> increaseEvents(int increaseNum) throws JsonProcessingException {
    List<Event> eventList = new ArrayList<>(increaseNum);

    for (int i=0; i<increaseNum; i++){
      // 制造随机数
      makingRandom();

      // 获取当前对象的json形态
      byte[] webDataJson = getJsonData();

      Event event = new SimpleEvent();
      event.setBody(webDataJson);

      eventList.add(event);
    }

    return eventList;
  }

  private void makingRandom() {
    String uuid = RandomUtil.getUuid();

    this.entrySID.setValue(uuid);
    this.entryWID.setValue(uuid);
    this.entryEVENTS_TM.setValue(RandomUtil.getTime());
    this.entryEVENTS_EA_cookieId.setValue(RandomUtil.getUuid());
    this.entryEVENTS_EA_playSessionId.setValue(RandomUtil.getUuid());
    this.entryEVENTS_EA_contentId.setValue(RandomUtil.getPid());
    this.entryEVENTS_EA_program_id.setValue(RandomUtil.getPid());
  }

  private byte[] getJsonData() throws JsonProcessingException {
    return objectMapper.writeValueAsBytes(this.webData);
  }

  private ArrayList<Map<String, Object>> getArrByKey(Map<String, Object> webData, String key) {
    return (ArrayList<Map<String, Object>>) webData.get(key);
  }

  private Map.Entry<String, Object> getEntryByKey(Map<String, Object> webData, String key) {
    for (Map.Entry<String, Object> e : webData.entrySet()){
      if (e.getKey().equals(key)){
        return e;
      }
    }

    return null;
  }

  private Map<String, Object> getWebDataMain() {
    return new HashMap<String, Object>(){{
      put("PORT","2233");
      put("APPID","132");
      put("IP","192.128.0.1");
      put("ISP","移动");
      put("UA","iOS11.4.1");
      put("PROV","浙江");
      put("SID","aaaas-sddadf-fewfwe-erwerwer-ewrewr");
      put("IPLAT","11.11");
      put("WID","aaaas-sddadf-fewfwe-erwerwer-ewrewr");
      put("IPLOC","111.111");
      put("COUNTRY","中国");
      put("LG","zh-cn");

      put("EVENTS",new ArrayList<Map<String, Object>>(){{
        add(new HashMap<String, Object>(){{
          put("PATH","https://www.ceshi.cn/aaaaa/bbbbb/ccccc/ddddd/19079?group_id=Game&page_id=19079&assignNo=176727&channel_id=10002_KY");
          put("ETP",0);
          put("EID","playHeartEvent");
          put("TM","1623144531873");
          put("EA",new ArrayList<Map<String, Object>>(){{
            add(new HashMap<String, Object>(){{
              put("EK","dataSource");
              put("EV","WAP");
            }});
            add(new HashMap<String, Object>(){{
              put("EK","cookieId");
              put("EV","abcde12345");
            }});
            add(new HashMap<String, Object>(){{
              put("EK","playSessionId");
              put("EV","abcde12345");
            }});
            add(new HashMap<String, Object>(){{
              put("EK","program_id");
              put("EV","713003824");
            }});
            add(new HashMap<String, Object>(){{
              put("EK","contentId");
              put("EV","713003824");
            }});
          }});
        }});
      }});
    }};
  }
}
