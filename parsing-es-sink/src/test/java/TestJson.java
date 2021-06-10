import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.junit.Test;
import per.cly.parsing_es_sink.ParsingEsSink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 测试各json工具速度
 * @author Chen768959
 * @date 2021/6/9
 */
public class TestJson {
  private static final String jsonStr = "{\"PAGES\":[],\"EVENTS\":[{\"PATH\":\"https://www.xxxxxxxxx.com/abc/defghsds/dfsd/sdfaLdew.html?mgdbId=12312313\",\"ETP\":2,\"EID\":\"weaffdasfasdgf\",\"TM\":13412343214124,\"EA\":[{\"EV\":\"https://www.xxxxxxx.com/sds/wqeqwewq/ewdsd/qweqwedqwdqw.html?mgdbId=123123123123\",\"EK\":\"url\"},{\"EV\":\"www.xxxxxxxxx.com\",\"EK\":\"dqwdqw\"},{\"EV\":\"测测测测\",\"EK\":\"title\"},{\"EV\":\"\",\"EK\":\"referrer\"},{\"EV\":\"2021-06-08 17:28\",\"EK\":\"timestamp\"},{\"EV\":\"Win\",\"EK\":\"os\"},{\"EV\":\"2131231231\",\"EK\":\"contentId\"},{\"EV\":\"\",\"EK\":\"promotionId\"},{\"EV\":\"ewdwedew\",\"EK\":\"product\"},{\"EV\":null,\"EK\":\"parameter1\"},{\"EV\":null,\"EK\":\"ks-udid\"},{\"EV\":null,\"EK\":\"ks-sessionid\"},{\"EV\":\"\",\"EK\":\"parameter2\"},{\"EV\":\"\",\"EK\":\"parameter3\"},{\"EV\":\"\",\"EK\":\"referer\"},{\"EV\":\"dewdwedaa\",\"EK\":\"app_from\"},{\"EV\":\"\",\"EK\":\"pageId\"},{\"EV\":\"213123124123123\",\"EK\":\"channelId\"},{\"EV\":\"\",\"EK\":\"account\"},{\"EV\":\"\",\"EK\":\"userId\"},{\"EV\":\"Chrome\",\"EK\":\"browser\"},{\"EV\":\"cdacasdfcasdfawefawefasfasdfasdfsadf\",\"EK\":\"sessionId\"},{\"EV\":0,\"EK\":\"platform\"},{\"EV\":\"12312312312231412423141234234\",\"EK\":\"busSessionId\"},{\"EV\":\"\",\"EK\":\"pageName\"},{\"EV\":\"\",\"EK\":\"blockName\"},{\"EV\":\"\",\"EK\":\"blockId\"},{\"EV\":\"\",\"EK\":\"positionIndex\"},{\"EV\":\"\",\"EK\":\"positionId\"},{\"EV\":\"\",\"EK\":\"positionName\"},{\"EV\":\"\",\"EK\":\"targetProgramId\"},{\"EV\":\"\",\"EK\":\"targetPageName\"},{\"EV\":\"\",\"EK\":\"uid\"},{\"EV\":\"234124234234234\",\"EK\":\"mgdbId\"},{\"EV\":\"WEB\",\"EK\":\"dataSource\"},{\"EV\":\"\",\"EK\":\"pwid\"},{\"EV\":\"23421342314\",\"EK\":\"program_id\"},{\"EV\":\"live\",\"EK\":\"videoType\"},{\"EV\":\"\",\"EK\":\"currentPageName\"},{\"EV\":\"\",\"EK\":\"currentPageId\"},{\"EV\":\"1\",\"EK\":\"mainsite\"},{\"EK\":\"needPromotion\"},{\"EV\":0,\"EK\":\"videoLength\"},{\"EV\":132423412,\"EK\":\"currentPoint\"},{\"EV\":3124234234,\"EK\":\"playDuration\"},{\"EV\":32413242,\"EK\":\"passbyDuration\"},{\"EV\":[],\"EK\":\"downloadTrace\"},{\"EV\":null,\"EK\":\"SubsessionServiceIP\"},{\"EV\":\"\",\"EK\":\"requestUrl\"},{\"EV\":3242.2342134324,\"EK\":\"bufferDuration\"},{\"EV\":0,\"EK\":\"m3u8DownLoadNum\"},{\"EV\":0,\"EK\":\"m3u8UpdateNum\"},{\"EV\":\"12342314234\",\"EK\":\"cid\"},{\"EV\":\"324234231412342342314234234\",\"EK\":\"playSessionId\"},{\"EV\":\"3\",\"EK\":\"rateType\"},{\"EV\":\"32412343214\",\"EK\":\"urlType\"},{\"EV\":\"https://wwwwwww.xxxxxxx.com:443/4324/324234234124dewdewd.flv?msisdn=ewfawefasdfasdfasdfasdgdsdaf&mdspid=&spid=424234&netType=0&sid=3214213423412&pid=324523543253&timestamp=3453425435435&Channel_ID=34253454353425345&ProgramID=34543534&ParentNodeID=-99&assertID=52345435435&client_ip=444.4.444.55&SecurityKey=412342342344&mvid=2141234124&mcid=1243214234&mpid=12342342134&playurlVersion=234123423dewdwe&userid=&jmhm=&videocodec=fewe&bean=fewfwe&encrypt=342342134123efwfdweweddew\\n\",\"EK\":\"playUrl\"},{\"EV\":\"333333\",\"EK\":\"version\"},{\"EV\":23423414123,\"EK\":\"liveEndTime\"},{\"EV\":null,\"EK\":\"currentFragSn\"},{\"EV\":2,\"EK\":\"streamType\"},{\"EV\":\"edwdwedwe\",\"EK\":\"type\"},{\"EV\":\"dewdwewqq\",\"EK\":\"ip\"},{\"EV\":\"e32ewqeqwde\",\"EK\":\"timestamp\"},{\"EV\":\"wqerqwerqwerwqer3241234123412342134\",\"EK\":\"cookieId\"},{\"EV\":\"https://www.dddwedede.com/eee/eeee/prd/ewrweqrweqr.html?mgdbId=3123432412342\",\"EK\":\"url\"},{\"EV\":\"www.dadasfdsfdsf.com\",\"EK\":\"efae\"},{\"EV\":\"反反复复\",\"EK\":\"title\"},{\"EV\":\"\",\"EK\":\"referrer\"},{\"EV\":3241234,\"EK\":\"screenHeight\"},{\"EV\":324234,\"EK\":\"screenWidth\"},{\"EV\":24,\"EK\":\"screenColorDepth\"},{\"EV\":\"zh-CN\",\"EK\":\"language\"},{\"EV\":\"dewdawedasdfasdfasdf2342134231423142314324dewd\",\"EK\":\"userAgent\"},{\"EV\":\"pc\",\"EK\":\"os\"}],\"NM\":\"\"}],\"PORT\":\"\",\"OS\":\"\",\"APPID\":\"9\",\"IP\":\"333.44421.334.55\",\"ISP\":\"单独\",\"UA\":\"\",\"PROV\":\"方法\",\"UDID\":\"\",\"SID\":\"ewfwqefweqfwqef32432142314dewwe\",\"IPLAT\":\"234.342\",\"WID\":\"ferfwaefsdafsaf3423rewfsdf\",\"IPLOC\":\"32432.432\",\"COUNTRY\":\"中国\",\"CITY\":\"西安\",\"UAG\":\"\",\"LG\":\"zh-cn\",\"TIMEZONE\":\"-8\",\"UAGV\":\"\"}";

  private static final String jsonRule = "{\"PORT\":\"port\",\"PROV\":\"prov\",\"WID\":\"wid\",\"SID\":\"sid\",\"CITY\":\"city\",\"IP\":\"ip\",\"COUNTRY\":\"country\",\"EVENTS\":[{\"PATH\":\"path\"}]}";

  private static final String jsonStr2 = "";
  private static final String jsonRule2 = "";
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

  // 1w 340222700ms
  public void jackson(String jsonStr){
    try {
      Map<String,String> resMap = new HashMap<>();

      JsonNode jsonNode = objectMapper.readTree(jsonStr);
      String port = jsonNode.get("PORT").asText();
      String prov = jsonNode.get("PROV").asText();
      String wid = jsonNode.get("WID").asText();
      String sid = jsonNode.get("SID").asText();
      String city = jsonNode.get("CITY").asText();
      String ip = jsonNode.get("IP").asText();
      String country = jsonNode.get("COUNTRY").asText();
      String s = jsonNode.get("EVENTS").get(0).get("PATH").asText();

      resMap.put("port",port);
      resMap.put("prov",prov);
      resMap.put("wid",wid);
      resMap.put("sid",sid);
      resMap.put("city",city);
      resMap.put("ip",ip);
      resMap.put("country",country);
      resMap.put("s",s);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static final Gson gson = new Gson();
  private static final JsonParser parser = new JsonParser();

  // 1w 355893900ms
  //    487056600ms
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


  @Test
  public void analysisTest(){
    ParsingEsSink parsingEsSink = new ParsingEsSink();
    parsingEsSink.testInit("completeNAME", jsonRule2);

    List<Event> eventBatch = new ArrayList<>();
    Event event = new SimpleEvent();
    event.setBody(jsonStr2.getBytes());
    eventBatch.add(event);

    long startTime=System.nanoTime();   //获取开始时间
    for (int i=0 ;i<1; i++){
      List<Map<String, String>> maps = parsingEsSink.testAnalysis(eventBatch);

      System.out.println(JSONArray.fromObject(maps));
    }

    long endTime=System.nanoTime(); //获取结束时间

    System.out.println("程序运行时间： " + (endTime - startTime ) + "ms");
  }

}
