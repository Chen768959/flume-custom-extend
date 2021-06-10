package per.cly.parsing_es_sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
import org.elasticsearch.action.delete.DeleteRequest;
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
import java.util.Optional;

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

  // 每条完整数据被存入es后的对应列名
  private String completeDataFieldName;

  // 一次从channel中取出的event数
  private int batchSize;

  // 公共规则，解析原始json数据的那些key，以及存放es的列名
  // key：原始json数据的key名及位置   value：对应es列名
  private JsonNode analysisJsonNodeRule;

  // 数组规则
  private JsonNode analysisJsonNodeArrRule;

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
        JsonNode eventJsonNode = null;
        try {
          eventJsonNode = objectMapper.readTree(eventBody);
        } catch (IOException e) {
          e.printStackTrace();
        }

        // 解析此event的公共属性
        Map<String, String> commonFieldMap = new HashMap<>();
        putCommonDataToMap(analysisJsonNodeRule, eventJsonNode, commonFieldMap);

        // 如果此event有array规则，则解析array中的每一个对象的属性，
        // 每个arr中的对象都会加上刚刚的公共属性生成新的map（也就是一条待写入es数据）
        if (! analysisJsonNodeArrKeyStructure.isEmpty()){
          /**
           * 1.先将待原始解析数据的json数组（oldArr）拷贝一份，作为newArr，
           * 2.然后删除oldArr中的所有元素，
           * 3.接着遍历newArr，解析每一个结果，
           * 4.每次遍历末尾处都将newArr中的遍历node存入oldArr中的第一项，然后再将oldArr所属的总node深拷贝一份，
           * 这样就拥有了包含公共数据与特定数组元素数据的总node
           */

          // 找到event中的待解析数组
          ArrayNode oldEventArrJsonNode = (ArrayNode) eventJsonNode.get(analysisJsonNodeArrKeyStructure.get(0));
          for (int i=1; i<analysisJsonNodeArrKeyStructure.size(); i++){
            oldEventArrJsonNode = (ArrayNode) oldEventArrJsonNode.get(i);
          }

          //1.先将待原始解析数据的json数组（oldArr）拷贝一份，作为newArr，
          ArrayNode newEventArrJsonNode = oldEventArrJsonNode.deepCopy();

          // 3.接着遍历newArr，解析每一个结果，
          // 解析数组中的每个待解析数据为新map
          for (JsonNode eventJsonNodeForArr : newEventArrJsonNode){
            Map<String, String> arrFieldMap = new HashMap<>();
            putCommonDataToMap(analysisJsonNodeArrRule, eventJsonNodeForArr, arrFieldMap);
            arrFieldMap.putAll(commonFieldMap);

            //将一个数组内的解析结果map作为一条待写入es数据
            //2.然后删除oldArr中的所有元素，
            //4.每次遍历末尾处都将newArr中的遍历node存入oldArr中的第一项，然后再将oldArr所属的总node深拷贝一份，
            oldEventArrJsonNode.removeAll();
            oldEventArrJsonNode.add(eventJsonNodeForArr);
            arrFieldMap.put(completeDataFieldName, eventJsonNode.deepCopy().toString());
            eventEsDataList.add(arrFieldMap);
          }
        }else {
          // 无数组规则，直接将公共信息作为一条待写入es数据
          commonFieldMap.put(completeDataFieldName, eventJsonNode.toString());
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
   * @param resultEsDataMap 解析结果，代表一条需存入es的数据
   * @author Chen768959
   * @date 2021/6/9 下午 10:19
   * @return void
   */
  private void putCommonDataToMap(JsonNode analysisJsonNodeConf, JsonNode eventJsonNode,
                                  Map<String, String> resultEsDataMap) {
    Iterator<Map.Entry<String, JsonNode>> eventRules = analysisJsonNodeConf.fields();

    while (eventRules.hasNext()){
      // key：规则json的key，与待解析数据的key相同
      // value：规则json的value，可能是一个新的对象规则，或者是“此数据存入es后的列名”
      Map.Entry<String, JsonNode> eventRule = eventRules.next();
      String fieldName = eventRule.getKey();
      JsonNode ruleNode = eventRule.getValue();

      if (!ruleNode.isArray()){
        if (ruleNode.isObject()){
          // 将子对象规则，与子待解析对象数据递归解析
          putCommonDataToMap(ruleNode, eventJsonNode.get(fieldName), resultEsDataMap);
        }else {
          try {
            resultEsDataMap.put(ruleNode.asText(), eventJsonNode.get(fieldName).asText());
          }catch (Exception e){
            throw new RuntimeException("异常 fieldName："+fieldName,e);
          }
        }
      }
    }
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
    BulkRequest request = new BulkRequest();
    for (BulkItemResponse item : items) {
      DeleteRequest deleteRequest = new DeleteRequest(esIndex, item.getId());
      request.add(deleteRequest);
    }

    try {
      esClient.bulk(request,RequestOptions.DEFAULT);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  /**
   * 从conf文件获取定义好的常量
   * @param context
   * @author Chen768959
   * @date 2021/6/9 下午 2:05
   * @return void
   */
  public void configure(Context context) {
    esIp = context.getString("es-ip");
    esPort = context.getInteger("es-port");
    esIndex = context.getString("es-index");
    userName = context.getString("user-name");
    password = context.getString("password");
    batchSize = context.getInteger("batch-size");
    completeDataFieldName = context.getString("complete-data-es-fname");

    try {
      analysisJsonNodeRule = objectMapper.readTree(context.getString("analysisJsonNodeRule"));

      checkIsArrays(analysisJsonNodeRule, false, null);
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
  private void checkIsArrays(JsonNode analysisJsonNodeConf, boolean hasArray, List<String> aJsonNodeArrKeyStructure) throws Exception {


    boolean nowHasArray = hasArray;
    Iterator<String> analysisJsonNodeNameIterator = analysisJsonNodeConf.fieldNames();

    while (analysisJsonNodeNameIterator.hasNext()){
      String analysisJsonNodeName = analysisJsonNodeNameIterator.next();
      JsonNode jsonNode = analysisJsonNodeConf.get(analysisJsonNodeName);

      if (jsonNode.isArray()){
        if (nowHasArray){
          throw new RuntimeException("json解析策略禁止配置多个数组");
        }else {
          aJsonNodeArrKeyStructure = Optional.ofNullable(aJsonNodeArrKeyStructure).orElse(new ArrayList<>());
          aJsonNodeArrKeyStructure.add(analysisJsonNodeName);
          this.analysisJsonNodeArrKeyStructure = aJsonNodeArrKeyStructure;
          this.analysisJsonNodeArrRule = jsonNode.get(0);
          nowHasArray = true;
        }
      }

      if (jsonNode.isObject()){
        ArrayList<String> nowAnalysisJsonNodeArrKeyStructure = new ArrayList<>(Optional.ofNullable(aJsonNodeArrKeyStructure).orElse(new ArrayList<>()));
        nowAnalysisJsonNodeArrKeyStructure.add(analysisJsonNodeName);
        checkIsArrays(jsonNode, nowHasArray,nowAnalysisJsonNodeArrKeyStructure);
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

  /**
   * 初始化es
   * @author Chen768959
   * @date 2021/6/10 上午 11:02
   * @return void
   */
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

  // 加载规则以及其中数组规则
  public void testInit(String completeDataFieldName, String analysisJsonNodeRule){
    this.completeDataFieldName = completeDataFieldName;

    try {
      this.analysisJsonNodeRule = objectMapper.readTree(analysisJsonNodeRule);

      checkIsArrays(this.analysisJsonNodeRule, false,null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public List<Map<String, String>> testAnalysis(List<Event> eventBatch){
    return getEventEsDataList(eventBatch);
  }
}