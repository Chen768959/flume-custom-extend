package parsing_es_sink;

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
   * 将eventJsonNode转化成待存入es的map数据，每一个map就是一条数据
   * @param eventJsonNode
   * @author Chen768959
   * @date 2021/6/17 上午 11:13
   * @return java.util.List<java.util.Map<java.lang.String,java.lang.String>>
   */
  List<Map<String,Object>> getEventEsDataList(JsonNode eventJsonNode);

  /**
   * 根据eventJsonNode获取索引名
   * @param eventJsonNode
   * @author Chen768959
   * @date 2021/6/17 上午 11:15
   * @return java.lang.String
   */
  String getEsIndex(JsonNode eventJsonNode);
}
