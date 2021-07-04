package parsing_es_sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.http.HttpHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parsing_es_sink.parsing.ParsingEsManager;
import parsing_es_sink.parsing.ParsingManagerSpecialAppImpl;
import parsing_es_sink.parsing.ParsingManagerSpecialWebImpl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Chen768959
 * @date 2021/6/9
 */
public class ParsingEsSink extends AbstractSink implements Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(ParsingEsSink.class);

  // es管理工具
  private EsManager641 esManager;

  // 一次从channel中取出的event数
  private int batchSize;

  private ParsingEsManager parsingAppEsManager;
  private ParsingEsManager parsingWebEsManager;

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
        // key是index，value为待写入数据
        Map<String,List<Map<String, Object>>> eventEsDataListMap = new HashMap<>();

        for (Event event : eventBatch){
          byte[] eventBody = event.getBody(); // 一个event中会包含多个需要被解析的事件数据

          try {
            JsonNode eventJsonNode = parsingAppEsManager.readTree(eventBody);

            if (parsingAppEsManager.checkFormat(eventJsonNode)){
              parsingAppEsManager.getEventEsDataList(eventJsonNode, eventEsDataListMap);
            }else if (parsingWebEsManager.checkFormat(eventJsonNode)){
              parsingWebEsManager.getEventEsDataList(eventJsonNode, eventEsDataListMap);
            }else {
              LOG.error("原数据格式异常无法解析："+new String(eventBody));
            }
          }catch (Exception e){
            LOG.error("解析event json异常，event_body："+new String(eventBody), e);
          }
        }

        // 相同esIndex的数据批量写入es
        eventEsDataListMap.entrySet().forEach(e->{
          boolean res = false;
          try {
            res = esManager.addAllData(e.getValue(),e.getKey());
          } catch (IOException ioException) {
            throw new RuntimeException("highLevelClient bulk add failed...will retry",ioException);
          }

          // 回退
          if (! res){
            throw new RuntimeException("highLevelClient bulk add failed...will retry，存在数据发送失败");
          }
        });

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
   * 从conf文件获取定义好的常量
   * @param context
   * @author Chen768959
   * @date 2021/6/9 下午 2:05
   * @return void
   */
  public void configure(Context context) {
    LOG.info("configure,读取配置");
    /**
     * ================================================================================================
     * es配置
     */
    String[] esHostArr = context.getString("es-host").split("\\,");
    HttpHost[] httpHosts = new HttpHost[esHostArr.length];
    for (int i=0;i<esHostArr.length;i++){
      String[] host = esHostArr[i].split("\\:");
      httpHosts[i] = new HttpHost(host[0],Integer.parseInt(host[1]),"http");
    }
    String userName = context.getString("user-name");
    String password = context.getString("password");
    batchSize = context.getInteger("batch-size");
    Integer indexNumberOfShards = context.getInteger("index-number-of-shards");
    Integer indexNumberOfReplicas = context.getInteger("index-number-of-replicas");
    String needDataFormatFieldNameListStr = context.getString("need-data-format-field-name-list");
    List<String> needDataFormatFieldNameList = null;
    if (needDataFormatFieldNameListStr!=null){
      needDataFormatFieldNameList = Arrays.asList(needDataFormatFieldNameListStr.split("\\,"));
    }
    String dataFormatStr = context.getString("data-format");
    SimpleDateFormat dataFormat = null;
    if (dataFormatStr!=null){
      dataFormat = new SimpleDateFormat(dataFormatStr);
    }

    // 设置es参数
    this.esManager = new EsManager641();
    esManager.initEs(userName, password, httpHosts, needDataFormatFieldNameList,
            dataFormat, indexNumberOfShards, indexNumberOfReplicas);

    /**
     * ================================================================================================
     * es解析工具配置
     */
    boolean Special = context.getBoolean("useParsingEsSpecial");
    if (Special){
      String indexPreStr = context.getString("indexPreStr");

      this.parsingAppEsManager = new ParsingManagerSpecialAppImpl(indexPreStr);
      this.parsingWebEsManager = new ParsingManagerSpecialWebImpl(indexPreStr);
    }else {
//      // 匹配es index 前缀和寻值规则
//      String esIndexRule = context.getString("esIndexRule");
//
//      // 匹配analysisJsonNodeRule
//      String analysisJsonNodeRule = context.getString("analysisJsonNodeRule");
//
//      // 匹配analysisValueJsonNodeRule
//      String analysisValueJsonNodeRuleGroup = context.getString("analysisValueJsonNodeRule");
//      Map<String, String> analysisValueJsonNodeRuleGroupMap = context.getSubProperties("analysisValueJsonNodeRule.");
//      Map<String, String> analysisValueJsonNodeRuleMap = null;
//      if (!analysisValueJsonNodeRuleGroupMap.isEmpty()) {
//        // key为rule1、rule2   value为规则
//        analysisValueJsonNodeRuleMap = selectByKeys(analysisValueJsonNodeRuleGroupMap,
//                analysisValueJsonNodeRuleGroup.split("\\s+"));
//      }
//
//      // 全量数据名称
//      String completeDataFieldName = context.getString("complete-data-es-fname");
//
//      // 设置ParsingEsManager
//      this.parsingEsManager = new ParsingEsManagerImpl(completeDataFieldName, esIndexRule, analysisJsonNodeRule, analysisValueJsonNodeRuleMap);
    }
  }

  private Map<String, String> selectByKeys(Map<String, String> map, String[] keys) {
    Map<String, String> result = Maps.newHashMap();
    for (String key : keys) {
      if (map.containsKey(key)) {
        result.put(key, map.get(key));
      }
    }
    return result;
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
   * TEST=========================================================================================================
   */
//
//  // 加载规则以及其中数组规则
//  public void testInit(String completeDataFieldName, String esIndexRuleStr, String analysisRule, Map<String, String> analysisValueJsonNodeRuleMap){
//
//    this.parsingEsManager = new ParsingEsManagerImpl(completeDataFieldName, esIndexRuleStr, analysisRule, analysisValueJsonNodeRuleMap);
//  }
//
//  public List<Map<String, Object>> testAnalysis(List<Event> eventBatch){
//    Map<String,List<Map<String, Object>>> eventEsDataList = new HashMap<>();
//
//    for (Event event : eventBatch){
//      byte[] eventBody = event.getBody(); // 一个event中会包含多个需要被解析的事件数据
//
//      try {
//        JsonNode eventJsonNode = parsingEsManager.readTree(eventBody);
//
//        parsingEsManager.getEventEsDataList(eventJsonNode,eventEsDataList);
//      }catch (Exception e){
//        throw new RuntimeException(e);
//      }
//    }
//
//    return eventEsDataList.values().iterator().next();
//  }
}