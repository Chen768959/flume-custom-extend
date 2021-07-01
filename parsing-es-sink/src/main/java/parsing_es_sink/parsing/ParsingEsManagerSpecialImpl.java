//package parsing_es_sink.parsing;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.node.ArrayNode;
//import com.fasterxml.jackson.databind.node.ObjectNode;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import static parsing_es_sink.parsing.ParsingEsManagerSpecialImpl.FieldConstants.AC;
//import static parsing_es_sink.parsing.ParsingEsManagerSpecialImpl.FieldConstants.ACCOUNT;
//import static parsing_es_sink.parsing.ParsingEsManagerSpecialImpl.FieldConstants.AVC;
//import static parsing_es_sink.parsing.ParsingEsManagerSpecialImpl.FieldConstants.DEVINFO;
//import static parsing_es_sink.parsing.ParsingEsManagerSpecialImpl.FieldConstants.DI;
//import static parsing_es_sink.parsing.ParsingEsManagerSpecialImpl.FieldConstants.EID;
//import static parsing_es_sink.parsing.ParsingEsManagerSpecialImpl.FieldConstants.ETM;
//import static parsing_es_sink.parsing.ParsingEsManagerSpecialImpl.FieldConstants.EVENT;
//import static parsing_es_sink.parsing.ParsingEsManagerSpecialImpl.FieldConstants.IMEI1;
//import static parsing_es_sink.parsing.ParsingEsManagerSpecialImpl.FieldConstants.IMSI1;
//import static parsing_es_sink.parsing.ParsingEsManagerSpecialImpl.FieldConstants.PHONE_NUM;
//
///**
// * @author Chen768959
// * @date 2021/6/17
// */
//public class ParsingEsManagerSpecialImpl implements ParsingEsManager{
//  private static final Logger LOG = LoggerFactory.getLogger(ParsingEsManagerSpecialImpl.class);
//
//  private final static ObjectMapper objectMapper = new ObjectMapper();
//
//  private EventTypeEnum eventTypeEnum;
//
//  private String indexPreStr;
//
//  public ParsingEsManagerSpecialImpl(String indexPreStr, String eventType){
//    if ("app".equals(eventType)){
//      this.eventTypeEnum = EventTypeEnum.APP;
//    }else if ("web".equals(eventType)){
//      this.eventTypeEnum = EventTypeEnum.WEB;
//    }else {
//      new RuntimeException("eventType异常，只支持'app'和'web'两种解析类型");
//    }
//
//    this.indexPreStr = indexPreStr;
//  }
//
//  @Override
//  public JsonNode readTree(byte[] eventBody) throws IOException {
//    return objectMapper.readTree(eventBody);
//  }
//
//  @Override
//  public List<Map<String, Object>> getEventEsDataList(JsonNode eventJsonNode) {
//    switch (this.eventTypeEnum){
//      case APP:
//        return getEventEsDataListForApp(eventJsonNode);
//      case WEB:
//        return getEventEsDataListForWeb(eventJsonNode);
//    }
//
//    return new ArrayList<>();
//  }
//
//  private List<Map<String, Object>> getEventEsDataListForApp(JsonNode eventJsonNode) {
//    List<Map<String, Object>> eventEsDataList = new ArrayList<>();
//
//    Map<String, Object> eventEsCommonData = new HashMap<>();
//    // 解析通常参数
//    JsonNode devInfoNode = eventJsonNode.get("DevInfo");
//    if (devInfoNode != null){
//      if (devInfoNode.isObject()){
//        eventEsCommonData.put(AVC,getText(devInfoNode, "AVC"));
//        eventEsCommonData.put(AC,getText(devInfoNode, "AC"));
//        eventEsCommonData.put(IMSI1,getText(devInfoNode, "IMSI1"));
//        eventEsCommonData.put(IMEI1,getText(devInfoNode, "IMEI1"));
//        eventEsCommonData.put(DI,getText(devInfoNode, "DI"));
//
//        eventEsCommonData.put(DEVINFO,devInfoNode.toString());
//      }
//    }
//
//    // 解析数组参数
//    JsonNode eventsNode = eventJsonNode.get("Events");
//    if (eventsNode != null){
//      if (eventsNode.isArray()){
//        // 迭代每一个数组内对象
//        for (JsonNode eventJsonNodeForArr : (ArrayNode)eventsNode){
//          Map<String, Object> eventEsForArrData = new HashMap<>();
//
//          eventEsForArrData.put(EID,getText(eventJsonNodeForArr,"EID"));
//          eventEsForArrData.put(ETM,getText(eventJsonNodeForArr,"ETM"));
//
//          // 寻找其中数组的电话和账户
//          JsonNode tdtNode = eventJsonNodeForArr.get("TDT");
//          if (tdtNode !=null){
//            if (tdtNode.isArray()){
//              boolean hasPhone = false;
//              boolean hasAcc = false;
//              for (JsonNode tdtNodeForArr : (ArrayNode)tdtNode){
//                String ek = getText(tdtNodeForArr, "EK");
//                if ("phone_number".equals(ek)){
//                  eventEsForArrData.put(PHONE_NUM,getText(tdtNodeForArr,"EV"));
//                  hasPhone = true;
//                }
//                if ("account".equals(ek)){
//                  eventEsForArrData.put(ACCOUNT,getText(tdtNodeForArr,"EV"));
//                  hasAcc = true;
//                }
//                if (hasPhone && hasAcc){
//                  break;
//                }
//              }
//            }
//          }
//
//          // 将event单独存储
//          eventEsForArrData.put(EVENT,eventJsonNodeForArr.toString());
//
//          // 合并通用属性
//          eventEsForArrData.putAll(eventEsCommonData);
//
//          eventEsDataList.add(eventEsForArrData);
//        }
//      }else {
//        eventEsDataList.add(eventEsCommonData);
//      }
//    }else {
//      eventEsDataList.add(eventEsCommonData);
//    }
//
//    return eventEsDataList;
//  }
//
//  private List<Map<String, Object>> getEventEsDataListForWeb(JsonNode eventJsonNode) {
//    List<Map<String, Object>> eventEsDataList = new ArrayList<>();
//
//    Map<String, Object> eventEsCommonData = new HashMap<>();
//    // 解析通常参数
//    eventEsCommonData.put(DI,getText(eventJsonNode, "WID"));
//    //除去EVENTS后的数据作为公共属性
//    ObjectNode tmpEventJsonNode = (ObjectNode)eventJsonNode.deepCopy();
//    tmpEventJsonNode.remove("EVENTS");
//    eventEsCommonData.put(DEVINFO, tmpEventJsonNode.toString());
//
//    // 解析数组参数
//    JsonNode eventsNode = eventJsonNode.get("EVENTS");
//    if (eventsNode != null){
//      if (eventsNode.isArray()){
//        // 迭代每一个数组内对象
//        for (JsonNode eventJsonNodeForArr : (ArrayNode)eventsNode){
//          Map<String, Object> eventEsForArrData = new HashMap<>();
//
//          eventEsForArrData.put(EID,getText(eventJsonNodeForArr,"EID"));
//          eventEsForArrData.put(ETM,getText(eventJsonNodeForArr,"TM"));
//
//          // 寻找其中数组的电话和账户
//          JsonNode eaNode = eventJsonNodeForArr.get("EA");
//          if (eaNode !=null){
//            if (eaNode.isArray()){
//              boolean hasPhone = false;
//              boolean hasAcc = false;
//              for (JsonNode eaNodeForArr : (ArrayNode)eaNode){
//                String ek = getText(eaNodeForArr, "EK");
//                if ("phone_number".equals(ek)){
//                  eventEsForArrData.put(PHONE_NUM,getText(eaNodeForArr,"EV"));
//                  hasPhone = true;
//                }
//                if ("account".equals(ek)){
//                  eventEsForArrData.put(ACCOUNT,getText(eaNodeForArr,"EV"));
//                  hasAcc = true;
//                }
//                if (hasPhone && hasAcc){
//                  break;
//                }
//              }
//            }
//          }
//
//          // 将event单独存储
//          eventEsForArrData.put(EVENT,eventJsonNodeForArr.toString());
//
//          // 合并通用属性
//          eventEsForArrData.putAll(eventEsCommonData);
//
//          eventEsDataList.add(eventEsForArrData);
//        }
//      }else {
//        eventEsDataList.add(eventEsCommonData);
//      }
//    }else {
//      eventEsDataList.add(eventEsCommonData);
//    }
//
//    return eventEsDataList;
//  }
//
//  @Override
//  public String getEsIndex(JsonNode eventJsonNode) {
//    switch (this.eventTypeEnum){
//      case APP:
//        return getEsIndexForApp(eventJsonNode);
//      case WEB:
//        return getEsIndexForWeb(eventJsonNode);
//    }
//
//    return this.indexPreStr;
//  }
//
//  private String getEsIndexForWeb(JsonNode eventJsonNode) {
//    try {
//      return this.indexPreStr + eventJsonNode.get("APPID").asText();
//    }catch (Exception e){
//      LOG.warn("event格式有误，解析appid异常",e);
//    }
//
//    return this.indexPreStr;
//  }
//
//  private String getEsIndexForApp(JsonNode eventJsonNode) {
//    try {
//      return this.indexPreStr + eventJsonNode.get("DevInfo").get("APPID").asText();
//    }catch (Exception e){
//      LOG.warn("event格式有误，解析appid异常",e);
//    }
//
//    return this.indexPreStr;
//  }
//
//  private String getText(JsonNode devInfoNode, String key) {
//    JsonNode jsonNode = devInfoNode.get(key);
//    if (jsonNode!=null){
//      return jsonNode.asText();
//    }
//    return "";
//  }
//
//  enum EventTypeEnum{
//    APP,
//    WEB
//  }
//}
