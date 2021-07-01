package parsing_es_sink.parsing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Chen768959
 * @date 2021/7/8
 */
public class ParsingTest extends ParsingManagerBase implements ParsingEsManager {
  @Override
  public void getEventEsDataList(JsonNode eventJsonNode, Map<String, List<Map<String, Object>>> eventEsDataListMap) throws JsonProcessingException {
    eventEsDataListMap.put("dialingtest_appid_20120708_1",new ArrayList<Map<String, Object>>(){{
      add(new HashMap<String, Object>(){{
        put("eid", "eid");
        put("etm", "1625751884000");
        put("phoneNum", "123456789");
        put("imei", "imei");
        put("imsi", "imsi");
        put("androidID", "androidID");
        put("idfa", "idfa");
        put("oaid", "oaid");
        put("udid", "udid");
        put("ip", "ip");
        put("channel", "channel");
        put("devinfo", new HashMap<String,String>(){{
          put("IMEI1","111");
          put("UDID","111");
          put("IMSI1","111");
          put("ANDROID","111");
          put("IDFA","111");
          put("PHONE","11");
          put("IP","111");
        }});
        put("event", new HashMap<String,String>(){{
          put("type","56000004");
          put("pageID","73b78ab30193471e811fce017ccbfa37");
        }});
      }});
    }});
  }
}
