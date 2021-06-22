package self_increase_source.data;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Map;

/**
 * @author Chen768959
 * @date 2021/6/22
 */
public abstract class BaseData {
  protected final ObjectMapper objectMapper = new ObjectMapper();

  protected ArrayList<Map<String, Object>> getArrByKey(Map<String, Object> data, String key) {
    return (ArrayList<Map<String, Object>>) data.get(key);
  }

  protected Map.Entry<String, Object> getEntryByKey(Map<String, Object> data, String key) {
    for (Map.Entry<String, Object> e : data.entrySet()){
      if (e.getKey().equals(key)){
        return e;
      }
    }

    return null;
  }

  protected Map.Entry<String, Object> getEntryByEK(ArrayList<Map<String, Object>> data, String ekKey) {
    for (Map<String, Object> map : data){
      if (map.get("EK").equals(ekKey)){
        return getEntryByKey(map,"EV");
      }
    }

    return null;
  }
}
