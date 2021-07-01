package parsing_es_sink.parsing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parsing_es_sink.Constants;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Chen768959
 * @date 2021/6/17
 */
public class ParsingManagerSpecialWebImpl extends ParsingManagerBase implements ParsingEsManager{
  private static final Logger LOG = LoggerFactory.getLogger(ParsingManagerSpecialWebImpl.class);
  private final String indexPreStr;
  private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");

  public ParsingManagerSpecialWebImpl(String indexPreStr){
    this.indexPreStr = indexPreStr;
  }

  @Override
  public void getEventEsDataList(JsonNode eventJsonNode, Map<String,List<Map<String, Object>>> eventEsDataListMap) {
    Map<String, Object> eventEsCommonData = new HashMap<>();
    // 解析通常参数
    eventEsCommonData.put(Constants.getInstance().getIp(),getText(eventJsonNode, "IP"));
    eventEsCommonData.put(Constants.getInstance().getIp(),getText(eventJsonNode, "APPID"));

    // 解析数组参数
    JsonNode eventsNode = eventJsonNode.get("EVENTS");
    if (eventsNode != null){
      if (eventsNode.isArray()){
        // 迭代每一个数组内对象
        for (JsonNode eventJsonNodeForArr : (ArrayNode)eventsNode){
          // 代表该条入es的数据
          Map<String, Object> eventEsForArrData = new HashMap<>();
          // 代表event指标，其为tdt中的每一项的k-v格式
          Map<String, String> eventTDT = new HashMap<>();

          eventEsForArrData.put(Constants.getInstance().getEid(),getText(eventJsonNodeForArr,"EID"));
          eventEsForArrData.put(Constants.getInstance().getEtm(),getText(eventJsonNodeForArr,"TM"));

          // 寻找其中数组的电话和账户
          JsonNode eaNode = eventJsonNodeForArr.get("EA");
          if (eaNode !=null){
            if (eaNode.isArray()){
              // 解析TDT中的每一项，并将其转换为k-v格式
              for (JsonNode tdtNodeForArr : (ArrayNode)eaNode){
                String ek = getText(tdtNodeForArr, "EK");
                String ev = getText(tdtNodeForArr, "EK");

                eventTDT.put(ek,ev);

                if ("phone_number".equals(ek)){
                  eventEsForArrData.put(Constants.getInstance().getPhoneNum(),getText(tdtNodeForArr,"EV"));
                }
              }
            }
          }

          // 将event单独存储
          eventEsForArrData.put(Constants.getInstance().getEvent(),eventTDT);

          // 合并通用属性
          eventEsForArrData.putAll(eventEsCommonData);

          // 设置devinfo
          eventEsForArrData.put(Constants.getInstance().getDevinfo(),new HashMap<String,Object>(){{
            put("PHONE",eventEsForArrData.get(Constants.getInstance().getPhoneNum()));
            put("IP",eventEsForArrData.get(Constants.getInstance().getIp()));
          }});

          // 寻找index，并设置
          String indexName = this.indexPreStr;
          try {
            long date = Long.parseLong((String) eventEsForArrData.get(Constants.getInstance().getEtm()));
            String formatDate = this.simpleDateFormat.format(date);

            indexName = this.indexPreStr+formatDate+"_"+eventEsForArrData.get(Constants.getInstance().getAppid());
          }catch (Exception e){
            LOG.error("解析indexName失败",e);
          }
          List<Map<String, Object>> indexNameMaps = eventEsDataListMap.get(indexName);
          if (indexNameMaps==null){
            eventEsDataListMap.put(indexName, new ArrayList<Map<String, Object>>(){{
              add(eventEsForArrData);
            }});
          }else {
            indexNameMaps.add(eventEsForArrData);
          }
        }
      }
    }
  }
}
