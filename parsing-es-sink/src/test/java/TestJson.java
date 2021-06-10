import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.sf.json.JSONObject;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;

/**
 * 测试各json工具速度
 * @author Chen768959
 * @date 2021/6/9
 */
public class TestJson {
  private static final String jsonStr = "";

  private static final ObjectMapper objectMapper = new ObjectMapper();
  @Test
  public void test(){
    long startTime=System.nanoTime();   //获取开始时间
    for (int i=0 ;i<10000; i++){
      jackson(jsonStr);
    }

    long endTime=System.nanoTime(); //获取结束时间

    System.out.println("程序运行时间： " + (endTime - startTime ) + "ms");
  }

  // 1w 5591611300ms
  public void sfJson(String jsonStr){
    JSONObject jsonObject = JSONObject.fromObject(jsonStr);
    jsonObject.getString("PORT");
    jsonObject.getString("PROV");
    jsonObject.getString("WID");
    jsonObject.getString("SID");
    jsonObject.getString("CITY");
    jsonObject.getString("IP");
    jsonObject.getString("COUNTRY");
    jsonObject.getJSONArray("EVENTS").getJSONObject(0).getString("PATH");
  }

  // 1w 326773600ms
  public void jackson(String jsonStr){
    try {
      JsonNode jsonNode = objectMapper.readTree(jsonStr);
      System.out.println(jsonNode.get("PORT").asText());
      System.out.println(jsonNode.get("PROV").asText());
      System.out.println(jsonNode.get("WID").asText());
      System.out.println(jsonNode.get("SID").asText());
      System.out.println(jsonNode.get("CITY").asText());
      System.out.println(jsonNode.get("IP").asText());
      System.out.println(jsonNode.get("COUNTRY").asText());
      System.out.println(jsonNode.get("EVENTS").get(0).get("PATH").asText());
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static final Gson gson = new Gson();
  private static final JsonParser parser = new JsonParser();

  // 1w 355893900ms
  public void gson(String jsonStr){
    JsonObject asJsonObject = parser.parse(jsonStr).getAsJsonObject();

    asJsonObject.get("PORT").getAsString();
    asJsonObject.get("PROV").getAsString();
    asJsonObject.get("WID").getAsString();
    asJsonObject.get("SID").getAsString();
    asJsonObject.get("CITY").getAsString();
    asJsonObject.get("IP").getAsString();
    asJsonObject.get("COUNTRY").getAsString();
    asJsonObject.get("EVENTS").getAsJsonArray().get(0).getAsJsonObject().get("PATH").getAsString();
  }

  @Test
  public void test2(){
    try {
      JsonNode jsonNode = objectMapper.readTree(jsonStr);


      Iterator<String> elements = jsonNode.fieldNames();

      while (elements.hasNext()){
        System.out.println(elements.next());
      }
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void copyTest(){
    try {
      JsonNode jsonNode = objectMapper.readTree(jsonStr);

      System.out.println(jsonNode.toString());
      long startTime=System.nanoTime();   //获取开始时间
      for (int i=0 ;i<10000; i++){
        JsonNode jsonNode1 = jsonNode.deepCopy();
      }

      long endTime=System.nanoTime(); //获取结束时间

      System.out.println("程序运行时间： " + (endTime - startTime ) + "ms");
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
  }
}
