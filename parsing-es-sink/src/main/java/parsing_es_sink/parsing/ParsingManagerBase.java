package parsing_es_sink.parsing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * @author Chen768959
 * @date 2021/7/6
 */
public abstract class ParsingManagerBase implements ParsingEsManager {
  protected final static ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public JsonNode readTree(byte[] eventBody) throws IOException {
    return objectMapper.readTree(eventBody);
  }

  protected String getText(JsonNode devInfoNode, String key) {
    JsonNode jsonNode = devInfoNode.get(key);
    if (jsonNode!=null){
      return jsonNode.asText();
    }
    return "NULL";
  }
}
