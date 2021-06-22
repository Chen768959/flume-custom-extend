package self_increase_source.data;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.Map;

/**
 * @author Chen768959
 * @date 2021/6/22
 */
public interface Data {
  Map<String,Object> getData();

  void makingRandom() throws JsonProcessingException;
}
