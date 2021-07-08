package parsing_es_sink.parsing;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parsing_es_sink.Constants;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
        analyseDevInfo(devInfoNode, eventEsCommonData);
      }
    }else {
      analyseDevInfo(eventJsonNode, eventEsCommonData);
    }

    // 解析数组参数
    JsonNode eventsNode = eventJsonNode.get("Events");
    if (eventsNode != null){
      if (eventsNode.isArray()){
        // 迭代每一个数组内对象
        events:for (JsonNode eventJsonNodeForArr : (ArrayNode)eventsNode){
          // 代表该条入es的数据
          Map<String, Object> eventEsForArrData = new HashMap<>();
          // 代表event指标，其为tdt中的每一项的k-v格式
          Map<String, String> eventTDT = new HashMap<>();

          String eid = getText(eventJsonNodeForArr, "EID");
          if (StringUtils.isEmpty(eid)){
            continue;
          }
          eventEsForArrData.put(Constants.getInstance().getEid(),eid);
          eventEsForArrData.put(Constants.getInstance().getEtm(),getText(eventJsonNodeForArr,"ETM"));

          // 寻找其中数组的电话和账户
          JsonNode tdtNode = eventJsonNodeForArr.get("TDT");
          if (tdtNode !=null){
            if (tdtNode.isObject()){
              // 水晶数据，解析data中的所有事件，每个事件作为一个event
              if ("crystal_data".equals(eid) && tdtNode.get("data")!=null){
                String crystalTDTdata = getText(tdtNode,"data");
                addCrystalTDTdata(crystalTDTdata, eventEsCommonData, eventEsDataListMap);
                continue events;
              }else {
                // 常规解析，TDT中的所有属性作为event，并取出手机号
                Iterator<Map.Entry<String, JsonNode>> tdtFields = tdtNode.fields();
                while (tdtFields.hasNext()){
                  Map.Entry<String, JsonNode> next = tdtFields.next();
                  if ("phone_number".equals(next.getKey())){
                    eventEsForArrData.put(Constants.getInstance().getPhoneNum(),next.getValue().asText());
                  }
                  if (StringUtils.isNotEmpty(next.getKey())){
                    eventTDT.put(next.getKey(),next.getValue().asText());
                  }
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
            put("IMEI1", Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getImei())).orElse(""));
            put("UDID",Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getUdid())).orElse(""));
            put("IMSI1",Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getImsi())).orElse(""));
            put("ANDROID",Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getAndroidID())).orElse(""));
            put("IDFA",Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getIdfa())).orElse(""));
            put("PHONE",Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getPhoneNum())).orElse(""));
            put("IP",Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getIp())).orElse(""));
            put("CHANNEL",Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getChannel())).orElse(""));
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

  private void addCrystalTDTdata(String crystalTDTdataStr, Map<String, Object> eventEsCommonData, Map<String, List<Map<String, Object>>> eventEsDataListMap) {
    String newCrystalTDTdata = crystalTDTdataStr.replaceAll("\\\\", "").replaceAll("\"\\{", "\\{").replaceAll("\\}\"", "\\}");
    JsonNode crystalTDTdata = null;
    try {
      crystalTDTdata = objectMapper.readTree(newCrystalTDTdata);
    } catch (JsonProcessingException e) {
      LOG.error("解析crystalTDTdata异常，原始数据："+crystalTDTdataStr+"...解析后数据："+newCrystalTDTdata,e);
      return;
    }

    if (crystalTDTdata.isObject()){
      JsonNode customEvent = crystalTDTdata.get("customEvent");
      JsonNode sdkSessionInfo = crystalTDTdata.get("sdkSessionInfo");
      //解析手机号
      String phoneNum = getText(sdkSessionInfo,"account");
      //解析每一个event
      if (customEvent!=null && customEvent.isArray()){
        for (JsonNode eventFromCustomEvent : (ArrayNode)customEvent){
          // 代表该条入es的数据
          Map<String, Object> eventEsForArrData = new HashMap<>();

          // 解析时间
          eventEsForArrData.put(Constants.getInstance().getEtm(),getText(eventFromCustomEvent, "timestamp"));

          // 解析eid
          try {
            JsonNode action = eventFromCustomEvent.get("eventParams").get("action");
            if (action==null){
              continue;
            }else {
              String type = getText(action, "type");
              if (StringUtils.isEmpty(type)){
                type = getText(action.get("nameValuePairs"),"type");
              }
              eventEsForArrData.put(Constants.getInstance().getEid(), "crystal_data."+type);
            }
          }catch (Exception e){
            LOG.error("解析crystal_data中data eid异常，事件详情："+eventFromCustomEvent.toString(),e);
            continue ;
          }

          // 创建一个对象作为event
          Map<String, Object> event = new LinkedHashMap<>();
          try {
            event.put("event",objectMapper.treeToValue(eventFromCustomEvent, Map.class));
            event.put("sdkInfo", objectMapper.treeToValue(sdkSessionInfo, Map.class));
          } catch (JsonProcessingException e) {
            LOG.error("eventFromCustomEvent转换map异常，原始数据："+eventFromCustomEvent,e);
          }

          // 将event单独存储
          eventEsForArrData.put(Constants.getInstance().getEvent(),event);

          // 合并通用属性
          eventEsForArrData.putAll(eventEsCommonData);

          // 设置devinfo
          eventEsForArrData.put(Constants.getInstance().getDevinfo(),new HashMap<String,Object>(){{
            put("IMEI1", Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getImei())).orElse(""));
            put("UDID",Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getUdid())).orElse(""));
            put("IMSI1",Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getImsi())).orElse(""));
            put("ANDROID",Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getAndroidID())).orElse(""));
            put("IDFA",Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getIdfa())).orElse(""));
            put("PHONE",Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getPhoneNum())).orElse(""));
            put("IP",Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getIp())).orElse(""));
            put("CHANNEL",Optional.ofNullable(eventEsForArrData.get(Constants.getInstance().getChannel())).orElse(""));
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

  @Override
  public boolean checkFormat(JsonNode eventJsonNode) {
    JsonNode eventsNode = eventJsonNode.get("Events");
    if (eventsNode != null){
      return true;
    }else {
      return false;
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

  private void analyseDevInfo(JsonNode devInfoNode, Map<String, Object> eventEsCommonData){
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
        if (dis.length>0){
          eventEsCommonData.put(Constants.getInstance().getAndroidID(),dis[dis.length-1]);

          eventEsCommonData.put(Constants.getInstance().getIdfa(),dis[0]);

          eventEsCommonData.put(Constants.getInstance().getOaid(),dis[0]);
        }
        break;
      case ANDROID_10_DOWN:
        String[] dis2 = getText(devInfoNode, "DI").split("\\-");
        if (dis2.length>0){
          eventEsCommonData.put(Constants.getInstance().getAndroidID(),dis2[dis2.length-1]);
        }
        break;
      case IOS:
        eventEsCommonData.put(Constants.getInstance().getIdfa(),getText(devInfoNode,"IDFA"));
        break;
    }
  }
}
