package parsing_es_sink.parsing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Chen768959
 * @date 2021/6/17
 */
public interface ParsingEsManager {

  /**
   * 将event数据转化成json
   * @param eventBody
   * @author Chen768959
   * @date 2021/6/17 上午 11:11
   * @return com.fasterxml.jackson.databind.JsonNode
   */
  JsonNode readTree(byte[] eventBody) throws IOException;

  /**
   * 将eventJsonNode转化成待存入es的map数据，每一个map就是一条数据，
   * 然后解析出该条数据写入的index。并放入总map
   * @param eventJsonNode
   * @param eventEsDataListMap key是index，value为待写入数据
   * @author Chen768959
   * @date 2021/7/8 下午 3:37
   * @return void
   */
  void getEventEsDataList(JsonNode eventJsonNode, Map<String,List<Map<String, Object>>> eventEsDataListMap) throws JsonProcessingException;

  /**
   * 判断数据是否属于当前格式
   * @param eventJsonNode
   * @author Chen768959
   * @date 2021/7/4 下午 4:33
   * @return boolean 属于则true
   */
  boolean checkFormat(JsonNode eventJsonNode);
}
