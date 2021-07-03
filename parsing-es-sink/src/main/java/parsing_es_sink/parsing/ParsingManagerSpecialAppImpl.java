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
public class ParsingManagerSpecialAppImpl extends ParsingManagerBase implements ParsingEsManager{
  private static final Logger LOG = LoggerFactory.getLogger(ParsingManagerSpecialAppImpl.class);
  private final String indexPreStr;
  private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");

  public ParsingManagerSpecialAppImpl(String indexPreStr){
    this.indexPreStr = indexPreStr;
  }

  @Override
  public void getEventEsDataList(JsonNode eventJsonNode, Map<String,List<Map<String, Object>>> eventEsDataListMap) {
    Map<String, Object> eventEsCommonData = new HashMap<>();
    // 解析通常参数
    JsonNode devInfoNode = eventJsonNode.get("DevInfo");
    if (devInfoNode != null){
      if (devInfoNode.isObject()){
        eventEsCommonData.put(Constants.getInstance().getImsi(),getText(devInfoNode, "IMSI1"));
        eventEsCommonData.put(Constants.getInstance().getImei(),getText(devInfoNode, "IMEI1"));

        eventEsCommonData.put(Constants.getInstance().getUdid(),getText(devInfoNode, "UDID"));

        eventEsCommonData.put(Constants.getInstance().getIp(),getText(devInfoNode, "IP"));

        eventEsCommonData.put(Constants.getInstance().getChannel(),getText(devInfoNode, "AC"));

        eventEsCommonData.put(Constants.getInstance().getAppid(),getText(devInfoNode, "APPID"));

        dvTypeEnum dvType = getDvType(devInfoNode);
        switch (dvType){
          case ANDROID_10_PRO:
            String[] dis = getText(devInfoNode, "DI").split("\\-");
            eventEsCommonData.put(Constants.getInstance().getAndroidID(),dis[dis.length-1]);

            eventEsCommonData.put(Constants.getInstance().getIdfa(),dis[0]);

            eventEsCommonData.put(Constants.getInstance().getOaid(),dis[0]);
            break;
          case ANDROID_10_DOWN:
            String[] dis2 = getText(devInfoNode, "DI").split("\\-");
            eventEsCommonData.put(Constants.getInstance().getAndroidID(),dis2[dis2.length-1]);
            break;
          case IOS:
            eventEsCommonData.put(Constants.getInstance().getIdfa(),getText(devInfoNode,"IDFA"));
            break;
        }
      }
    }

    // 解析数组参数
    JsonNode eventsNode = eventJsonNode.get("Events");
    if (eventsNode != null){
      if (eventsNode.isArray()){
        // 迭代每一个数组内对象
        for (JsonNode eventJsonNodeForArr : (ArrayNode)eventsNode){
          // 代表该条入es的数据
          Map<String, Object> eventEsForArrData = new HashMap<>();
          // 代表event指标，其为tdt中的每一项的k-v格式
          Map<String, String> eventTDT = new HashMap<>();

          eventEsForArrData.put(Constants.getInstance().getEid(),getText(eventJsonNodeForArr,"EID"));
          eventEsForArrData.put(Constants.getInstance().getEtm(),getText(eventJsonNodeForArr,"ETM"));

          // 寻找其中数组的电话和账户
          JsonNode tdtNode = eventJsonNodeForArr.get("TDT");
          if (tdtNode !=null){
            if (tdtNode.isArray()){
              // 解析TDT中的每一项，并将其转换为k-v格式
              for (JsonNode tdtNodeForArr : (ArrayNode)tdtNode){
                String ek = getText(tdtNodeForArr, "EK");
                String ev = getText(tdtNodeForArr, "EV");

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
            put("IMEI1",eventEsForArrData.get(Constants.getInstance().getImei()));
            put("UDID",eventEsForArrData.get(Constants.getInstance().getUdid()));
            put("IMSI1",eventEsForArrData.get(Constants.getInstance().getImsi()));
            put("ANDROID",eventEsForArrData.get(Constants.getInstance().getAndroidID()));
            put("IDFA",eventEsForArrData.get(Constants.getInstance().getIdfa()));
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

  enum dvTypeEnum{
    ANDROID_10_PRO,
    ANDROID_10_DOWN,
    IOS,
    UNKNOW
  }

  private dvTypeEnum getDvType(JsonNode devInfoNode){
    if (getText(devInfoNode,"SN").equals("Android")){
      String sv = getText(devInfoNode, "SV");
      try {
        String version = sv.split("\\.")[0];
        if (Integer.parseInt(version) >= 10){
          return dvTypeEnum.ANDROID_10_PRO;
        }else {
          return dvTypeEnum.ANDROID_10_DOWN;
        }
      }catch (Exception e){
        LOG.error("解析SV异常，sv："+sv,e);
        return dvTypeEnum.UNKNOW;
      }
    }else {
      return dvTypeEnum.IOS;
    }
  }
}
