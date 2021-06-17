package parsing_es_sink;

import org.apache.commons.lang.StringUtils;
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
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 管理es连接
 * @author Chen768959
 * @date 2021/6/17
 */
public class EsManager {
  private static final Logger LOG = LoggerFactory.getLogger(EsManager.class);

  // es客户端
  private RestHighLevelClient esClient;

  // 需要转化时间格式的字段
  private List<String> needDataFormatFieldNameList;

  // 时间格式
  private SimpleDateFormat dataFormat;

  private int indexNumberOfShards;

  private int indexNumberOfReplicas;

  /**
   * 初始化es
   * @author Chen768959
   * @date 2021/6/10 上午 11:02
   * @return void
   */
  public void initEs(String userName, String password, HttpHost[] httpHosts, List<String> needDataFormatFieldNameList,
                      SimpleDateFormat dataFormat, int indexNumberOfShards, int indexNumberOfReplicas){
    this.needDataFormatFieldNameList = needDataFormatFieldNameList;
    this.dataFormat = dataFormat;
    this.indexNumberOfShards = indexNumberOfShards;
    this.indexNumberOfReplicas = indexNumberOfReplicas;

    //初始化ES操作客户端
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
            new UsernamePasswordCredentials(userName, password));  //es账号密码（默认用户名为elastic）
    esClient =new RestHighLevelClient(
            RestClient.builder(httpHosts).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
              public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                httpClientBuilder.disableAuthCaching();
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
              }
            })
    );
  }

  /**
   * 批量写入es
   * @param eventEsDataList
   * @author Chen768959
   * @date 2021/6/9 下午 6:16
   * @return boolean 写入成功返回true
   */
  public boolean addAllData(List<Map<String, String>> eventEsDataList, String esIndex) throws IOException {
    if (!Optional.ofNullable(eventEsDataList).orElse(new ArrayList<>()).isEmpty()){
      // 判断index是否存在
      GetIndexRequest getIndexRequest=new GetIndexRequest(esIndex);
      try{
        boolean exists=esClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        if (! exists){
          // 创建index
          createIndex(esIndex);
        }
      }catch (Exception e){
        LOG.error("判断index是否存在失败",e);
      }


      BulkRequest request = new BulkRequest();

      eventEsDataList.forEach(eventEsData->{
        // 处理需要修改时间格式的字段
        preDataFormat(eventEsData);

        request.add(new IndexRequest(esIndex).source(eventEsData).opType(DocWriteRequest.OpType.CREATE));
      });

      BulkResponse bulk = esClient.bulk(request, RequestOptions.DEFAULT);

      for (BulkItemResponse bulkItemResponse : bulk.getItems()){
        if (bulkItemResponse.isFailed()){
          //删除刚刚写入的数据
          rollbackBulkItemResponses(bulk.getItems(), esIndex);
          return false;
        }
      }
    }
    return true;
  }

  /**
   * 按指定格式转化指定列的时间戳
   * @param eventEsData
   * @author Chen768959
   * @date 2021/6/16 下午 7:11
   * @return void
   */
  private void preDataFormat(Map<String, String> eventEsData) {
    if (needDataFormatFieldNameList != null && dataFormat !=null){
      needDataFormatFieldNameList.forEach(needDataFormatFieldName->{
        String needDataFormatValue = eventEsData.get(needDataFormatFieldName);
        if (StringUtils.isNotEmpty(needDataFormatValue)){
          try {
            Date date = new Date(Long.parseLong(needDataFormatValue));
            eventEsData.put(needDataFormatFieldName, dataFormat.format(date));
          }catch (Exception e){
            LOG.error("时间格式转换异常，fieldName："+needDataFormatFieldName+"fieldValue："+needDataFormatValue, e);
          }
        }
      });
    }
  }

  /**
   * 创建index
   * @param esIndex
   * @author Chen768959
   * @date 2021/6/15 上午 9:47
   * @return void
   */
  private void createIndex(String esIndex) {
    CreateIndexRequest request=new CreateIndexRequest(esIndex);
    request.settings(Settings.builder().put("index.number_of_shards", indexNumberOfShards).put("index.number_of_replicas", indexNumberOfReplicas));

    Map<String, Object> message = new HashMap<>();
    message.put("type", "text");
    Map<String, Object> properties = new HashMap<>();
    properties.put("message", message);
    Map<String, Object> mapping = new HashMap<>();
    mapping.put("properties", properties);
    request.mapping(mapping);

    try {
      CreateIndexResponse createIndexResponse = esClient.indices().create(request, RequestOptions.DEFAULT);
      boolean acknowledged = createIndexResponse.isAcknowledged();
      boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
      if(acknowledged && shardsAcknowledged) {
        LOG.info("索引创建成功，index-name："+esIndex);
      }
    } catch (Exception e) {
      LOG.error("索引创建失败，index-name："+esIndex,e);
    }
  }

  /**
   * 回退响应数据
   * @param items
   * @author Chen768959
   * @date 2021/6/9 下午 8:05
   * @return void
   */
  private void rollbackBulkItemResponses(BulkItemResponse[] items, String esIndex) {
    BulkRequest request = new BulkRequest();
    for (BulkItemResponse item : items) {
      DeleteRequest deleteRequest = new DeleteRequest(esIndex, item.getId());
      request.add(deleteRequest);
    }

    try {
      esClient.bulk(request,RequestOptions.DEFAULT);
    } catch (Exception e) {
      LOG.info("es回退数据失败",e);
    }
  }
}
