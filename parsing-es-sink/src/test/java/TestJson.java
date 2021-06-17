import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;
import parsing_es_sink.ParsingEsSink;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 测试各json工具速度
 * @author Chen768959
 * @date 2021/6/9
 */
public class TestJson {
  private static final String jsonStr = "";

  private static final String jsonData2 = "";

  private static final String jsonRule2 = "";

  private static final Map<String,String> speJsonRule2 = new HashMap<String, String>(){{
    put("","");
    put("","");
  }};

  private static final ObjectMapper objectMapper = new ObjectMapper();
  @Test
  public void test(){
    long startTime=System.currentTimeMillis();   //获取开始时间
    for (int i=0 ;i<10000; i++){
      jackson(jsonData2);
    }

    long endTime=System.currentTimeMillis(); //获取结束时间

    System.out.println("程序运行时间： " + (endTime - startTime ) + "ms");
  }

  // 1w 5592ms
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

  // 1w 1475ms
  public void jackson(String jsonStr){
    try {
      JsonNode jsonNode = objectMapper.readTree(jsonStr);

      JsonNode jsonNode2 = jsonNode.get("DevInfo");
      String avc = jsonNode2.get("AVC").asText();
      String ac = jsonNode2.get("AC").asText();
      String IMSI1 = jsonNode2.get("IMSI1").asText();
      String IMEI1 = jsonNode2.get("IMEI1").asText();
      String di = jsonNode2.get("DI").asText();

      ArrayNode arrayNodeEvents = (ArrayNode) jsonNode.get("Events");
      for (JsonNode jsonNode1 : arrayNodeEvents){
        Map<String,String> resMap = new HashMap<>();

        resMap.put("eid",jsonNode1.get("EID").asText());
        resMap.put("etm",jsonNode1.get("ETM").asText());
        resMap.put("avc",avc);
        resMap.put("ac",ac);
        resMap.put("IMSI1",IMSI1);
        resMap.put("IMEI1",IMEI1);
        resMap.put("di",di);

        ArrayNode dtd = (ArrayNode) jsonNode1.get("TDT");
        for (JsonNode d : dtd){
          if ("phone_number".equals(d.get("EK").asText())){
            if (d.get("EV") !=null){
              resMap.put("ev",d.get("EV").asText());
              break;
            }
          }
        }

        for (JsonNode d : dtd){
          if ("account".equals(d.get("EK").asText())){
            if (d.get("EV") !=null){
              resMap.put("ev",d.get("EV").asText());
              break;
            }
          }
        }
      }
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static final Gson gson = new Gson();
  private static final JsonParser parser = new JsonParser();

  // 1w 356ms
  public void gson(String jsonStr){
    JsonObject asJsonObject = parser.parse(jsonStr).getAsJsonObject();

    asJsonObject.get("PORT").getAsString();
    asJsonObject.get("PROV").getAsString();
    asJsonObject.get("WID").getAsString();
    asJsonObject.get("SID").getAsString();
    asJsonObject.get("CITY").getAsString();
    asJsonObject.get("IP").getAsString();
    asJsonObject.get("COUNTRY").getAsString();
    String asString = asJsonObject.get("EVENTS").getAsJsonArray().get(0).getAsJsonObject().get("PATH").getAsString();
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


  // 1w 3485ms
  @Test
  public void analysisTest(){
    ParsingEsSink parsingEsSink = new ParsingEsSink();
    parsingEsSink.testInit("completeNAME","{\"DevInfo\":{\"APPID\":\"appid_\"}}", jsonRule2,speJsonRule2);

    List<Event> eventBatch = new ArrayList<>();
    Event event = new SimpleEvent();
    event.setBody(jsonData2.getBytes());
    eventBatch.add(event);

    long startTime=System.currentTimeMillis();   //获取开始时间
    for (int i=0 ;i<1; i++){
      List<Map<String, String>> maps = parsingEsSink.testAnalysis(eventBatch);
      System.out.println(JSONArray.fromObject(maps).toString());
    }

    long endTime=System.currentTimeMillis(); //获取结束时间

    System.out.println("程序运行时间： " + (endTime - startTime ) + "ms");
  }

  @Test
  public void format(){
    String format = "yyyy-MM-dd HH:mm:ss";
    String etm = "1623144244889";

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
    Date date = new Date(Long.parseLong(etm));
    String res = simpleDateFormat.format(date);
    System.out.println(res);
  }

  private static RestHighLevelClient restHighLevelClient;

  static {
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic", "operation"));
    RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost("192.168.209.128", 9200, "http"))
            .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
              @Override
              public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
              }
            });
    restHighLevelClient = new RestHighLevelClient(restClientBuilder);
  }


  /**
   * 判断某个index是否存在
   *
   * @param idxName index名
   * @return boolean
   * @throws
   * @since
   */
  public static boolean isExistsIndex(String idxName) throws Exception {
    return restHighLevelClient.indices().exists(new GetIndexRequest(idxName), RequestOptions.DEFAULT);
  }

  public static void main(String[] aaa){
//    Map<String,String> eventEsData = new HashMap<>();
//    eventEsData.put("中文","666");
//
//    BulkRequest request = new BulkRequest();
//
//    request.add(new IndexRequest("eeeee3").source(eventEsData).opType(DocWriteRequest.OpType.CREATE));
//
//    try {
//      BulkResponse bulk = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
//    } catch (IOException e) {
//      e.printStackTrace();
//    }

    String indexName = "AAAA3";
    try {
      System.out.println(isExistsIndex(indexName));
      if (! isExistsIndex(indexName)){
        CreateIndexRequest request=new CreateIndexRequest(indexName);
        request.settings(Settings.builder().put("index.number_of_shards", "3").put("index.number_of_replicas", "1"));

        Map<String, Object> message = new HashMap<>();
        message.put("type", "text");
        Map<String, Object> properties = new HashMap<>();
        properties.put("message", message);
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("properties", properties);
        request.mapping(mapping);

        try {
          CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
          boolean acknowledged = createIndexResponse.isAcknowledged();
          boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
          if(acknowledged && shardsAcknowledged) {
            System.out.println("索引创建成功，index-name："+indexName);
          }
        } catch (IOException e) {
          System.out.println("索引创建失败，index-name："+indexName);
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }finally {
      try {
        restHighLevelClient.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
