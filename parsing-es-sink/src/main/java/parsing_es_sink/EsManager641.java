package parsing_es_sink;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * 管理es连接
 * @author Chen768959
 * @date 2021/6/17
 */
public class EsManager641 {
  private static final Logger LOG = LoggerFactory.getLogger(EsManager641.class);

  // es客户端
  private RestHighLevelClient esClient;

  // 需要转化时间格式的字段
  private List<String> needDataFormatFieldNameList;

  // 时间格式
  private SimpleDateFormat dataFormat;

  // index Shards数量
  private int indexNumberOfShards;

  // index Replicas数量
  private int indexNumberOfReplicas;

  // 当前es中已有index名的缓存
  private Set<String> indexNameCache = new HashSet<>();

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
    if (userName!=null && password !=null){
      credentialsProvider.setCredentials(AuthScope.ANY,
              new UsernamePasswordCredentials(userName, password));  //es账号密码（默认用户名为elastic）
    }
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
  public boolean addAllData(List<Map<String, Object>> eventEsDataList, String esIndex) throws IOException {
    if (!Optional.ofNullable(eventEsDataList).orElse(new ArrayList<>()).isEmpty()){
      // 判断index是否存在
      try{
        if (! checkIndexExists(esIndex)){
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

        request.add(new IndexRequest(esIndex, "_doc",Constants.getInstance().getUUID()).source(eventEsData).opType(DocWriteRequest.OpType.CREATE));
      });

      try {
        // 写入es
        BulkResponse bulk = esClient.bulk(request, RequestOptions.DEFAULT);
        for (BulkItemResponse bulkItemResponse : bulk){
          if (bulkItemResponse.isFailed()){
            LOG.error("写入es失败，msg："+bulkItemResponse.getFailureMessage());
          }
        }
      }catch (ElasticsearchStatusException e){
        LOG.error("写入es失败,后续尝试重建index",e);

        try{
          if (! checkIndexExists(esIndex)){
            // 创建index
            createIndex(esIndex);
          }
        }catch (Exception e2){
          LOG.error("判断index是否存在失败",e2);
        }

        // 重试
        BulkResponse bulk = esClient.bulk(request, RequestOptions.DEFAULT);
        for (BulkItemResponse bulkItemResponse : bulk){
          if (bulkItemResponse.isFailed()){
            LOG.error("写入es失败，msg："+bulkItemResponse.getFailureMessage());
          }
        }
      }

    }
    return true;
  }

  private boolean checkIndexExists(String index) throws IOException {
    if (indexNameCache.contains(index)){
      return true;
    } else {
      boolean flag = false;
      try {
        flag =esClient.indices().exists(
                new GetIndexRequest()
                        .indices(new String[]{index}));

        if (flag){
          LOG.info("index已存在，index："+index);
          indexNameCache.add(index);
        }
      } catch (ElasticsearchException e) {
        LOG.error("checkIndexExists异常",e);
      }
      return flag;
    }

//    if (indexNameCache.contains(index)){
//      return true;
//    }else {
//      GetRequest getRequest = new GetRequest(index, "_doc",index);
//
//      boolean exists = esClient.exists(getRequest, RequestOptions.DEFAULT);
//
//      if (exists){
//        LOG.info("index已存在，index："+index);
//        indexNameCache.add(index);
//      }
//
//      return exists;
//    }
  }

  /**
   * 按指定格式转化指定列的时间戳
   * @param eventEsData
   * @author Chen768959
   * @date 2021/6/16 下午 7:11
   * @return void
   */
  private void preDataFormat(Map<String, Object> eventEsData) {
    if (needDataFormatFieldNameList != null && dataFormat !=null){
      needDataFormatFieldNameList.forEach(needDataFormatFieldName->{
        String needDataFormatValue = (String) eventEsData.get(needDataFormatFieldName);
        if (StringUtils.isNotEmpty(needDataFormatValue)){
          try {
            Date date = new Date(Long.parseLong(needDataFormatValue));
            String res = dataFormat.format(date);
            eventEsData.put(needDataFormatFieldName, res);
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
    CreateIndexRequest createIndexRequest = new CreateIndexRequest(esIndex);

    //1:settings

    HashMap settings_map = new HashMap(2);

    settings_map.put("number_of_shards", indexNumberOfShards);

    settings_map.put("number_of_replicas", indexNumberOfReplicas);

    createIndexRequest.settings(settings_map);

//2:mappings

    try {
      XContentBuilder builder = XContentFactory.jsonBuilder()

              .startObject()
              .startObject("properties")

              .startObject("devinfo")
              .field("type", "object")
              .endObject()

              .startObject("event")
              .field("type", "object")
              .endObject()

              .startObject("etm")
              .field("type", "date")
              .field("format", "yyyy/MM/dd HH:mm:ss")
              .endObject()

              .endObject()

              .endObject();

      createIndexRequest.mapping("_doc",builder);
    } catch (IOException e) {
      LOG.error("",e);
    }

    try {
      esClient.indices().create(createIndexRequest);
      LOG.info("索引创建成功，index-name："+esIndex);
    } catch (IOException e) {
      LOG.error("索引创建失败，index-name："+esIndex,e);
    }


//    Map<String, Object> mapping = new HashMap<String, Object>(){{
//      put("mappings",new HashMap<String, Object>(){{
//        put("_doc",new HashMap<String, Object>(){{
//          put("properties",new HashMap<String, Object>(){{
//            put("devinfo",new HashMap<String, Object>(){{
//              put("type", "object");
//            }});
//
//            put("event",new HashMap<String, Object>(){{
//              put("type", "object");
//            }});
//
//            // 设置时间字段格式
//            if (needDataFormatFieldNameList != null){
//              needDataFormatFieldNameList.forEach(needDataFormatFieldName->{
//                //将时间字段与时间格式绑定
//                put(needDataFormatFieldName, new HashMap<String, Object>(){{
//                  put("type", "date");
//                  put("format","yyyy-MM-dd HH:mm:ss");
//                }});
//              });
//            }
//          }});
//        }});
//      }});
//    }};
//
//    IndexRequest indexRequest = new IndexRequest(esIndex, "_doc",esIndex).source(mapping);
//
//    try {
//      IndexResponse indexResponse = esClient.index(indexRequest, RequestOptions.DEFAULT);
//
//      LOG.info("索引创建成功，status："+indexResponse.status());
//    } catch (IOException e) {
//      LOG.error("索引创建失败，index-name："+esIndex,e);
//    }
  }
}
