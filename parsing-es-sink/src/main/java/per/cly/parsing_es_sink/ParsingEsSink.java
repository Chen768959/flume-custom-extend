package per.cly.parsing_es_sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Chen768959
 * @date 2021/6/9
 */
public class ParsingEsSink extends AbstractSink implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(ParsingEsSink.class);

  // es ip
  private String esIp;

  // es port
  private int esPort;

  // esIndex
  private String esIndex;

  private String userName;

  private String password;

  // 一次从channel中取出的event数
  private int batchSize;

  // 公共规则，解析原始json数据的那些key，以及存放es的列名
  // key：原始json数据的key名及位置   value：对应es列名
  private JsonNode analysisJsonNodeConf;

  // 数组规则
  private JsonNode analysisJsonNodeArrConf;

  // 数组规则存在的key位置，如果此规则存在，则表示一个json数据会按照此规则解析出多个带写入es的map，且每个map都要包含公共map属性
  private List<String> analysisJsonNodeArrKeyStructure = new ArrayList<>();

  // es客户端
  private RestHighLevelClient esClient;

  private final static ObjectMapper objectMapper = new ObjectMapper();

  public Status process() throws EventDeliveryException {
    Status status = Status.READY;

    // 获取Channel对象
    Channel channel = getChannel();
    // 获取事务对象
    Transaction transaction = channel.getTransaction();

    transaction.begin();

    try {
      // 一次性处理batch-size个event对象
      List<Event> eventBatch = Lists.newLinkedList();
      for (int i = 0; i < batchSize; i++) {
        Event event = channel.take();

        if (event == null) {
          break;
        }
        eventBatch.add(event);
      }

      if (eventBatch.isEmpty()) {
        // BACKOFF表示让flume睡眠一段时间（因为此时已经取不出来event了）
        status =  Status.BACKOFF;

        // process
      } else {

        // 获取待存入es的数据集
        List<Map<String,String>> eventEsDataList = getEventEsDataList(eventBatch);

        // 批量写入
        boolean res = addAllData(eventEsDataList);

        if (! res){
          throw new RuntimeException("highLevelClient bulk add failed...will retry");
        }

        // READY表示event可以提交了
        status =  Status.READY;
      }

      // 如果执行成功，最后一定要提交
      transaction.commit();
    } catch (Throwable th) {
      transaction.rollback();
      LOG.error("ParsingEsSink process failed", th);
      if (th instanceof Error) {
        throw (Error) th;
      } else {
        throw new EventDeliveryException(th);
      }
    }finally {
      // 在关闭事务之前，必须执行过事务的提交或回退
      transaction.close();
    }

    // 将状态返回出去
    return status;
  }

  /**
   * 获取待存入es的数据集
   * @param eventBatch
   * @author Chen768959
   * @date 2021/6/9 下午 9:28
   * @return java.util.List<java.util.Map<java.lang.String,java.lang.String>> 一个map对象就是一条待写入数据
   */
  private List<Map<String, String>> getEventEsDataList(List<Event> eventBatch) {
    List<Map<String, String>> eventEsDataList = new ArrayList<>();

    for (Event event : eventBatch){
      byte[] eventBody = event.getBody(); // 一个event中会包含多个需要被解析的事件数据

      try {
        JsonNode eventJsonNode = objectMapper.readTree(eventBody);

        // 解析此event的公共属性
        Map<String, String> commonFieldMap = new HashMap<>();
        putCommonDataToMap(analysisJsonNodeConf, eventJsonNode, commonFieldMap);

        // 如果此event有array规则，则解析array中的每一个对象的属性，
        // 每个arr中的对象都会加上刚刚的公共属性生成新的map（也就是一条待写入es数据）
        if (! analysisJsonNodeArrKeyStructure.isEmpty()){
          // 找到event中的待解析数组
          JsonNode eventArrJsonNode = eventJsonNode.get(analysisJsonNodeArrKeyStructure.get(0));
          for (int i=1; i<analysisJsonNodeArrKeyStructure.size(); i++){
            eventArrJsonNode = eventArrJsonNode.get(i);
          }
          // 解析数组中的每个待解析数据为新map
          if (eventArrJsonNode.isArray()){
            Iterator<JsonNode> eventArrJsonNodeIterator = eventArrJsonNode.elements();
            while (eventArrJsonNodeIterator.hasNext()){
              Map<String, String> arrFieldMap = new HashMap<>();
              JsonNode eventJsonNodeForArr = eventArrJsonNodeIterator.next();
              putCommonDataToMap(analysisJsonNodeArrConf, eventJsonNodeForArr, arrFieldMap);
              arrFieldMap.putAll(commonFieldMap);

              //将一个数组内的解析结果map作为一条待写入es数据
              eventEsDataList.add(arrFieldMap);
            }
          }else {
            throw new RuntimeException("指定数组规则异常，不符合实际数据");
          }
        }else {
          // 无数组规则，直接将公共信息作为一条待写入es数据
          eventEsDataList.add(commonFieldMap);
        }
      } catch (Exception e) {
        LOG.error("解析event json异常，event_body："+eventBody, e);
      }
    }

    return eventEsDataList;
  }

  /**
   * 解析属性进入map
   * @param analysisJsonNodeConf 规则
   * @param eventJsonNode 待解析数据
   * @param commonFieldMap 解析结果，代表一条需存入es的数据
   * @author Chen768959
   * @date 2021/6/9 下午 10:19
   * @return void
   */
  private void putCommonDataToMap(JsonNode analysisJsonNodeConf, JsonNode eventJsonNode, Map<String, String> commonFieldMap) {

  }

  /**
   * 根据规则解析一个json数据
   * @param analysisJsonNodeConf
   * @param eventJsonNode
   * @author Chen768959
   * @date 2021/6/9 下午 9:36
   * @return java.util.List<java.util.Map<java.lang.String,java.lang.String>>
   */
  private List<Map<String, String>> getEsDataListForEvent(JsonNode analysisJsonNodeConf, JsonNode eventJsonNode) {
    Iterator<String> eventJsonFieldNameIterator = eventJsonNode.fieldNames();
    while (eventJsonFieldNameIterator.hasNext()){ // 迭代规则的每一个key
      String eventJsonFieldName = eventJsonFieldNameIterator.next();
    }

    Map<String, String> esDataMap = new HashMap<>();

    return null;
  }

  /**
   * 批量写入es
   * @param eventEsDataList
   * @author Chen768959
   * @date 2021/6/9 下午 6:16
   * @return boolean 写入成功返回true
   */
  private boolean addAllData(List<Map<String, String>> eventEsDataList) throws IOException {
    BulkRequest request = new BulkRequest();

    eventEsDataList.forEach(eventEsData->{
      request.add(new IndexRequest(esIndex).source(eventEsData).opType(DocWriteRequest.OpType.CREATE));
    });

    BulkResponse bulk = esClient.bulk(request, RequestOptions.DEFAULT);

    for (BulkItemResponse bulkItemResponse : bulk.getItems()){
      if (bulkItemResponse.isFailed()){
        //删除刚刚写入的数据
        rollbackBulkItemResponses(bulk.getItems());
        return false;
      }
    }

    return true;
  }

  /**
   * 回退响应数据
   * @param items
   * @author Chen768959
   * @date 2021/6/9 下午 8:05
   * @return void
   */
  private void rollbackBulkItemResponses(BulkItemResponse[] items) {

  }

  /**
   * 从conf文件获取定义好的常量
   * @param context
   * @author Chen768959
   * @date 2021/6/9 下午 2:05
   * @return void
   */
  public void configure(Context context) {
    esIp = context.getString("esIp");
    esPort = context.getInteger("esPort");
    esIndex = context.getString("esIndex");
    userName = context.getString("userName");
    password = context.getString("password");
    batchSize = context.getInteger("batch-size");

    try {
      analysisJsonNodeConf = objectMapper.readTree(context.getString("analysisJsonNodeConf"));

      checkIsArrays(analysisJsonNodeConf, false);
    } catch (Exception e) {
      LOG.error("aJsonNodeConf解析异常", e);
      stop();
    }

    initEs();
  }

  /**
   * 检查analysisJsonNodeConf中是否有配置多个数组
   * @param analysisJsonNodeConf
   * @param hasArray 当前解析配置中是否已经有数组配置了
   * @author Chen768959
   * @date 2021/6/9 下午 9:57
   * @return void
   */
  private void checkIsArrays(JsonNode analysisJsonNodeConf, boolean hasArray) throws Exception {
    boolean nowHasArray = hasArray;
    Iterator<String> analysisJsonNodeNameIterator = analysisJsonNodeConf.fieldNames();

    while (analysisJsonNodeNameIterator.hasNext()){
      String analysisJsonNodeName = analysisJsonNodeNameIterator.next();
      JsonNode jsonNode = analysisJsonNodeConf.get(analysisJsonNodeName);

      if (jsonNode.isArray()){
        if (nowHasArray){
          throw new RuntimeException("json解析策略禁止配置多个数组");
        }else {
          analysisJsonNodeArrConf = jsonNode.get(0);
          nowHasArray = true;
        }
      }

      if (jsonNode.isObject()){
        checkIsArrays(jsonNode, nowHasArray);
      }
    }
  }

  @Override
  public synchronized void stop() {
    super.stop();
  }


  @Override
  public synchronized void start() {
    try {
      super.start();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  private void initEs(){
    //当es用用户名和密码连接时
    //初始化ES操作客户端
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials(userName, password));  //es账号密码（默认用户名为elastic）
    esClient =new RestHighLevelClient(
            RestClient.builder(new HttpHost(esIp,esPort,"http")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
              public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                httpClientBuilder.disableAuthCaching();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
              }
            })
    );
  }
}