//package parsing_es_sink.parsing;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.fasterxml.jackson.databind.node.ArrayNode;
//import com.fasterxml.jackson.databind.node.TextNode;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//
///**
// * @author Chen768959
// * @date 2021/6/17
// */
//public class ParsingEsManagerImpl implements ParsingEsManager{
//  // 每条完整数据被存入es后的对应列名
//  private String completeDataFieldName;
//
//  // esIndex前缀
//  private String esIndexPre;
//
//  // esIndex匹配规则
//  private List<KeyStructure> esIndexRuleLinkedMap;
//
//  // 公共规则，解析原始json数据的那些key，以及存放es的列名
//  // key：原始json数据的key名及位置   value：对应es列名
//  private JsonNode analysisJsonNodeRule;
//
//  // 数组规则
//  private JsonNode analysisJsonNodeArrRule;
//
//  // 数组规则存在的key位置，如果此规则存在，则表示一个json数据会按照此规则解析出多个带写入es的map，且每个map都要包含公共map属性
//  private List<String> analysisJsonNodeArrKeyStructure = new ArrayList<>();
//
//  // 特殊配规则，需要按照指定的key规则匹配value（之前全部是key匹配），找到符合此key的value值后，将同对象下的指定key的value作为结果值
//  private List<AnalysisValueJsonNodeRule> analysisValueJsonNodeRuleList = new ArrayList<>();
//
//  private final static ObjectMapper objectMapper = new ObjectMapper();
//
//  public ParsingEsManagerImpl(String completeDataFieldName, String esIndexRuleStr, String analysisJsonNodeRuleStr, Map<String, String> analysisValueJsonNodeRuleMap){
//    this.completeDataFieldName = completeDataFieldName;
//
//    // 匹配es index 前缀和寻值规则
//    try {
//      JsonNode esIndexRule = objectMapper.readTree(esIndexRuleStr);
//
//      // 解析出所有的key结构
//      this.esIndexRuleLinkedMap = getRuleKeyLinkedMap(esIndexRule);
//
//      // 根据key结构获取此结构对应需要匹配的最终结果值
//      this.esIndexPre = getValueByKeyStructure(esIndexRuleLinkedMap, esIndexRule);
//    } catch (Exception e) {
//      throw new RuntimeException("esIndexRule解析异常", e);
//    }
//
//    // 匹配analysisJsonNodeRule
//    try {
//      analysisJsonNodeRule = objectMapper.readTree(analysisJsonNodeRuleStr);
//
//      checkIsArrays(analysisJsonNodeRule, false, null);
//    } catch (Exception e) {
//      throw new RuntimeException("aJsonNodeConf解析异常", e);
//    }
//
//    // 匹配analysisValueJsonNodeRule
//    if (! Optional.ofNullable(analysisValueJsonNodeRuleMap).orElse(new HashMap<>()).isEmpty()){
//      analysisValueJsonNodeRuleMap.values().forEach(nodeRuleStr->{
//        int maoIndex = nodeRuleStr.length()-1;
//
//        for (; maoIndex>=0; maoIndex--){
//          if (nodeRuleStr.charAt(maoIndex) == ':'){
//            break;
//          }
//        }
//
//        if (maoIndex>=0){
//          String jsonRule = nodeRuleStr.substring(0,maoIndex);
//          String matchValueAndKey = nodeRuleStr.substring(maoIndex+1, nodeRuleStr.length());
//
//          try {
//            JsonNode ruleJsonNode = objectMapper.readTree(jsonRule);
//
//            // 解析出所有的key结构
//            List<KeyStructure> keyStructureList = getRuleKeyLinkedMap(ruleJsonNode);
//
//            // 根据key结构获取此结构对应需要匹配的最终结果值
//            String esFieldName = getValueByKeyStructure(keyStructureList, ruleJsonNode);
//
//            // 确定analysisJsonNodeRule中是否存在数组，如果存在，则判断其数组结构与ruleKeyLinkedMap是否重叠，
//            // 如果重叠，则将ruleKeyLinkedMap缩减为数组内的元素的结构，这样后续匹配时直接匹配数组内的元素
//            reduceRuleKeyLinkedMap(analysisJsonNodeRule, keyStructureList);
//
//            AnalysisValueJsonNodeRule analysisValueJsonNodeRule = new AnalysisValueJsonNodeRule();
//            analysisValueJsonNodeRule.setMatchStr(matchValueAndKey.split("\\,")[0]);
//            analysisValueJsonNodeRule.setResKeyName(matchValueAndKey.split("\\,")[1]);
//            analysisValueJsonNodeRule.setEsFieldName(esFieldName);
//            analysisValueJsonNodeRule.setKeyStructureList(keyStructureList);
//
//            analysisValueJsonNodeRuleList.add(analysisValueJsonNodeRule);
//          }catch (Exception e){
//            throw new RuntimeException("解析value匹配配置异常",e);
//          }
//        }else {
//          throw new RuntimeException("value匹配配置有误");
//        }
//
//      });
//    }
//  }
//
//  @Override
//  public JsonNode readTree(byte[] eventBody) throws IOException {
//    return objectMapper.readTree(eventBody);
//  }
//
//  /**
//   * 获取待存入es的数据集
//   * @param eventJsonNode
//   * @author Chen768959
//   * @date 2021/6/11 下午 6:14
//   * @return java.util.List<java.util.Map<java.lang.String,java.lang.String>> 一个map对象就是一条待写入数据
//   */
//  @Override
//  public List<Map<String, Object>> getEventEsDataList(JsonNode eventJsonNode) {
//    List<Map<String, Object>> eventEsDataList = new ArrayList<>();
//
//    // 解析此event的公共属性
//    Map<String, Object> commonFieldMap = new HashMap<>();
//    putCommonDataToMap(analysisJsonNodeRule, eventJsonNode, commonFieldMap);
//
//    // 如果此event有array规则，则解析array中的每一个对象的属性，
//    // 每个arr中的对象都会加上刚刚的公共属性生成新的map（也就是一条待写入es数据）
//    if (! analysisJsonNodeArrKeyStructure.isEmpty()){
//      /**
//       * 1.先将待原始解析数据的json数组（oldArr）拷贝一份，作为newArr，
//       * 2.然后删除oldArr中的所有元素，
//       * 3.接着遍历newArr，解析每一个结果，
//       * 4.每次遍历末尾处都将newArr中的遍历node存入oldArr中的第一项，然后再将oldArr所属的总node深拷贝一份，
//       * 这样就拥有了包含公共数据与特定数组元素数据的总node
//       */
//
//      // 找到event中的待解析数组
//      ArrayNode oldEventArrJsonNode = (ArrayNode) eventJsonNode.get(analysisJsonNodeArrKeyStructure.get(0));
//
//      for (int i=1; i<analysisJsonNodeArrKeyStructure.size(); i++){
//        oldEventArrJsonNode = (ArrayNode) oldEventArrJsonNode.get(i);
//      }
//
//      if (oldEventArrJsonNode != null){
//        //1.先将待原始解析数据的json数组（oldArr）拷贝一份，作为newArr，
//        ArrayNode newEventArrJsonNode = oldEventArrJsonNode.deepCopy();
//
//        // 3.接着遍历newArr，解析每一个结果，
//        // 解析数组中的每个待解析数据为新map
//        for (JsonNode eventJsonNodeForArr : newEventArrJsonNode){
//          Map<String, Object> arrFieldMap = new HashMap<>();
//          putCommonDataToMap(analysisJsonNodeArrRule, eventJsonNodeForArr, arrFieldMap);
//          arrFieldMap.putAll(commonFieldMap);
//
//          //将一个数组内的解析结果map作为一条待写入es数据
//          //2.然后删除oldArr中的所有元素，
//          //4.每次遍历末尾处都将newArr中的遍历node存入oldArr中的第一项，然后再将oldArr所属的总node深拷贝一份，
//          oldEventArrJsonNode.removeAll();
//          oldEventArrJsonNode.add(eventJsonNodeForArr);
//          arrFieldMap.put(completeDataFieldName, eventJsonNode.deepCopy().toString());
//
//          // 寻找特殊规则匹配结果（此时的analysisValueJsonNodeRule规则对应的就是数组内部的单元素结构）
//          for (AnalysisValueJsonNodeRule analysisValueJsonNodeRule : analysisValueJsonNodeRuleList){
//            String resValue = getResValueByAnalysisValueJsonNodeRule(analysisValueJsonNodeRule.getKeyStructureList(),
//                    analysisValueJsonNodeRule.getMatchStr(),
//                    analysisValueJsonNodeRule.getResKeyName(),
//                    eventJsonNodeForArr);
//            arrFieldMap.put(analysisValueJsonNodeRule.esFieldName, resValue);
//          }
//
//          eventEsDataList.add(arrFieldMap);
//        }
//      }else {
//        eventEsDataList.add(commonFieldMap);
//      }
//    }else {
//      // 无数组规则，直接将公共信息作为一条待写入es数据
//      commonFieldMap.put(completeDataFieldName, eventJsonNode.toString());
//      // 寻找特殊规则匹配结果
//      for (AnalysisValueJsonNodeRule analysisValueJsonNodeRule : analysisValueJsonNodeRuleList){
//        String resValue = getResValueByAnalysisValueJsonNodeRule(analysisValueJsonNodeRule.getKeyStructureList(),
//                analysisValueJsonNodeRule.getMatchStr(),
//                analysisValueJsonNodeRule.getResKeyName(),
//                eventJsonNode);
//        commonFieldMap.put(analysisValueJsonNodeRule.esFieldName, resValue);
//      }
//
//      eventEsDataList.add(commonFieldMap);
//    }
//
//    return eventEsDataList;
//  }
//
//  /**
//   * 计算esIndex
//   * @param eventJsonNode
//   * @author Chen768959
//   * @date 2021/6/11 下午 6:16
//   * @return java.lang.String
//   */
//  @Override
//  public String getEsIndex(JsonNode eventJsonNode) {
//    String indexAfter = getValueByKeyStructure(this.esIndexRuleLinkedMap, eventJsonNode);
//    if (indexAfter != null){
//      return this.esIndexPre+indexAfter;
//    }else {
//      return this.esIndexPre;
//    }
//  }
//
//  /**
//   * 解析属性进入map
//   * @param analysisJsonNodeConf 规则
//   * @param eventJsonNode 待解析数据
//   * @param resultEsDataMap 解析结果，代表一条需存入es的数据
//   * @author Chen768959
//   * @date 2021/6/9 下午 10:19
//   * @return void
//   */
//  private void putCommonDataToMap(JsonNode analysisJsonNodeConf, JsonNode eventJsonNode,
//                                  Map<String, Object> resultEsDataMap) {
//    Iterator<Map.Entry<String, JsonNode>> eventRules = analysisJsonNodeConf.fields();
//
//    while (eventRules.hasNext()){
//      // key：规则json的key，与待解析数据的key相同
//      // value：规则json的value，可能是一个新的对象规则，或者是“此数据存入es后的列名”
//      Map.Entry<String, JsonNode> eventRule = eventRules.next();
//      String fieldName = eventRule.getKey();
//      JsonNode ruleNode = eventRule.getValue();
//
//      if (!ruleNode.isArray()){
//        if (ruleNode.isObject()){
//          // 将子对象规则，与子待解析对象数据递归解析
//          putCommonDataToMap(ruleNode, eventJsonNode.get(fieldName), resultEsDataMap);
//        }else {
//          try {
//            resultEsDataMap.put(ruleNode.asText(), Optional.ofNullable(eventJsonNode.get(fieldName)).orElse(new TextNode("")).asText());
//          }catch (Exception e){
//            throw new RuntimeException("异常 fieldName："+fieldName,e);
//          }
//        }
//      }
//    }
//  }
//
//
//  /**
//   * 确定analysisJsonNodeRule中是否存在数组，如果存在，则判断其数组结构与ruleKeyLinkedMap是否重叠，
//   * 如果重叠，则将ruleKeyLinkedMap缩减为数组内的元素的结构，这样后续匹配时直接匹配数组内的元素
//   * @param analysisJsonNodeRule
//   * @param keyStructureList
//   * @author Chen768959
//   * @date 2021/6/11 下午 5:39
//   * @return void
//   */
//  private void reduceRuleKeyLinkedMap(JsonNode analysisJsonNodeRule, List<KeyStructure> keyStructureList) {
//    Iterator<KeyStructure> keyStructureIterator = keyStructureList.iterator();
//    JsonNode nowJsonNode = analysisJsonNodeRule;
//
//    while (keyStructureIterator.hasNext()){
//      KeyStructure keyStructure = keyStructureIterator.next();
//
//      JsonNode jsonNode = nowJsonNode.get(keyStructure.getKeyName());
//      if (jsonNode != null){
//        switch (keyStructure.getAnalysisValueRuleKeyEnum()){
//          // 如果当前结构key为对象key，则判断jsonNode是否为对象，为对象则表示匹配成功，删除此结构，继续循环
//          case ObjectKey:
//            if (jsonNode.isObject()){
//              nowJsonNode = jsonNode;
//              keyStructureIterator.remove();
//              continue;
//            }
//            break;
//          case ArrKey:
//            if (jsonNode.isArray()){
//              nowJsonNode = jsonNode.get(0);
//              keyStructureIterator.remove();
//              continue;
//            }
//            break;
//          case StringKey:
//            break;
//        }
//      }
//
//      break;
//    }
//  }
//
//  /**
//   * 1、根据“keyStructureList”规则找到“jsonNode”中的匹配规则的key
//   * 2、判断刚刚找到的key的value是否等于“targetKeyValue”
//   * 如果不等于则返回null
//   * 如果相等则表示找到了。
//   * 3、找到value后，则查看相同jsonNode对象中是否有key为“resKeyName”
//   * 4、如果此key也匹配，则此key的value就是该方法最终要找的结果
//   * 如果未匹配则返回null
//   * @param keyStructureList
//   * @param targetKeyValue
//   * @param resKeyName
//   * @param jsonNode 待解析数据
//   * @author Chen768959
//   * @date 2021/6/11 下午 8:34
//   * @return java.lang.String
//   */
//  private String getResValueByAnalysisValueJsonNodeRule(List<KeyStructure> keyStructureList, String targetKeyValue,
//                                                        String resKeyName , JsonNode jsonNode) {
//    String resValue = null;
//    Iterator<KeyStructure> keyStructureIterator = keyStructureList.iterator();
//
//    it: while (keyStructureIterator.hasNext()){
//      KeyStructure nextKeyStructure = keyStructureIterator.next();
//      JsonNode nowJsonNode = jsonNode.get(nextKeyStructure.getKeyName());
//
//      if (nowJsonNode != null){
//        switch (nextKeyStructure.getAnalysisValueRuleKeyEnum()){
//          case ObjectKey:
//            if (nowJsonNode.isObject()){
//              // 匹配成功，删除此key规则
//              keyStructureIterator.remove();
//              // 使用剩余规则继续匹配当前对象
//              resValue = getResValueByAnalysisValueJsonNodeRule(keyStructureList, targetKeyValue, resKeyName, nowJsonNode);
//            }
//            break it;
//          case ArrKey:
//            if (nowJsonNode.isArray()){
//              // 匹配成功，删除此key规则
//              keyStructureIterator.remove();
//              // 循环数组中的每个对象，直到找到resValue结果
//              ArrayNode arrayNode = (ArrayNode) nowJsonNode;
//              for (JsonNode jNode: arrayNode){
//                resValue = getResValueByAnalysisValueJsonNodeRule(keyStructureList, targetKeyValue, resKeyName, jNode);
//                if (resValue != null){
//                  break it;
//                }
//              }
//            }
//            break it;
//          case StringKey:
//            // 找到了目标key的value
//            String value = nowJsonNode.asText();
//            // 相等则找到了key，并且其value也匹配目标
//            if (targetKeyValue.equals(value)){
//              // 接着找value的父对象“jsonNode”中是否含有，真正的“结果key”
//              JsonNode resJsonNode = jsonNode.get(resKeyName);
//              if (resJsonNode != null){
//                resValue = resJsonNode.asText();
//              }
//            }
//            break it;
//        }
//      }
//    }
//
//    return resValue;
//  }
//
//  /**
//   * 根据匹配规则，找到jsonNode中的符合规则的key的value
//   * 如果遇到数组，则默认匹配其第0位
//   * @param keyStructureList 待匹配key的结构规则
//   * @param jsonNode 待解析json
//   * @author Chen768959
//   * @date 2021/6/11 下午 5:01
//   * @return java.lang.String
//   */
//  private String getValueByKeyStructure(List<KeyStructure> keyStructureList, JsonNode jsonNode) {
//    JsonNode nowJsonNode = jsonNode;
//    String resValue = null;
//
//    for (KeyStructure keyStructure : keyStructureList){
//      switch (keyStructure.analysisValueRuleKeyEnum){
//        case ObjectKey:
//          nowJsonNode = nowJsonNode.get(keyStructure.keyName);
//          break;
//        case ArrKey:
//          nowJsonNode = nowJsonNode.get(keyStructure.keyName).get(0);
//          break;
//        case StringKey:
//          resValue = nowJsonNode.get(keyStructure.keyName).asText();
//          break;
//      }
//    }
//
//    return resValue;
//  }
//
//  /**
//   * 解析出所有的key结构，
//   * 只为找出目的key-value
//   * （此方法只能找到第一个需要被找到的k-v的key，会记录沿途object和arr的结构）
//   * @param jsonNode
//   * @author Chen768959
//   * @date 2021/6/11 下午 7:40
//   * @return java.util.List<per.cly.parsing_es_sink.KeyStructure>
//   */
//  private List<KeyStructure> getRuleKeyLinkedMap(JsonNode jsonNode) {
//    List<KeyStructure> keyStructureList = new ArrayList<>();
//
//    Iterator<String> fieldNamesIterator = jsonNode.fieldNames();
//    while (fieldNamesIterator.hasNext()){
//      String fieldName = fieldNamesIterator.next();
//      JsonNode jsonNodeByName = jsonNode.get(fieldName);
//
//      if (jsonNodeByName.isObject()){
//        keyStructureList.add(new KeyStructure(fieldName, AnalysisValueRuleKeyEnum.ObjectKey));
//        keyStructureList.addAll(getRuleKeyLinkedMap(jsonNodeByName));
//        continue;
//      }
//
//      if (jsonNodeByName.isArray()){
//        keyStructureList.add(new KeyStructure(fieldName, AnalysisValueRuleKeyEnum.ArrKey));
//        keyStructureList.addAll(getRuleKeyLinkedMap(jsonNodeByName.get(0)));
//        continue;
//      }
//
//      keyStructureList.add(new KeyStructure(fieldName, AnalysisValueRuleKeyEnum.StringKey));
//      break;
//    }
//
//    return keyStructureList;
//  }
//
//  /**
//   * 检查analysisJsonNodeConf中是否有配置多个数组
//   * @param analysisJsonNodeConf
//   * @param hasArray 当前解析配置中是否已经有数组配置了
//   * @author Chen768959
//   * @date 2021/6/9 下午 9:57
//   * @return void
//   */
//  private void checkIsArrays(JsonNode analysisJsonNodeConf, boolean hasArray,
//                             List<String> aJsonNodeArrKeyStructure) throws Exception {
//    boolean nowHasArray = hasArray;
//    Iterator<String> analysisJsonNodeNameIterator = analysisJsonNodeConf.fieldNames();
//
//    while (analysisJsonNodeNameIterator.hasNext()){
//      String analysisJsonNodeName = analysisJsonNodeNameIterator.next();
//      JsonNode jsonNode = analysisJsonNodeConf.get(analysisJsonNodeName);
//
//      if (jsonNode.isArray()){
//        if (nowHasArray){
//          throw new RuntimeException("json解析策略禁止配置多个数组");
//        }else {
//          aJsonNodeArrKeyStructure = Optional.ofNullable(aJsonNodeArrKeyStructure).orElse(new ArrayList<>());
//          aJsonNodeArrKeyStructure.add(analysisJsonNodeName);
//          this.analysisJsonNodeArrKeyStructure = aJsonNodeArrKeyStructure;
//          this.analysisJsonNodeArrRule = jsonNode.get(0);
//          nowHasArray = true;
//        }
//      }
//
//      if (jsonNode.isObject()){
//        ArrayList<String> nowAnalysisJsonNodeArrKeyStructure = new ArrayList<>(Optional.ofNullable(aJsonNodeArrKeyStructure).orElse(new ArrayList<>()));
//        nowAnalysisJsonNodeArrKeyStructure.add(analysisJsonNodeName);
//        checkIsArrays(jsonNode, nowHasArray,nowAnalysisJsonNodeArrKeyStructure);
//      }
//    }
//  }
//
//  class AnalysisValueJsonNodeRule {
//    // 需被匹配的value的所处位置规则
//    List<KeyStructure> keyStructureList;
//
//    // 匹配成功后，同对象下，此key值的value将作为结果，此处为“此key值名”
//    private String resKeyName;
//
//    private String esFieldName;
//
//    // 该规则制定的key对应的value需要匹配的内容
//    private String matchStr;
//
//    public List<KeyStructure> getKeyStructureList() {
//      return new ArrayList<KeyStructure>(keyStructureList);
//    }
//
//    public void setKeyStructureList(List<KeyStructure> keyStructureList) {
//      this.keyStructureList = keyStructureList;
//    }
//
//    public String getEsFieldName() {
//      return esFieldName;
//    }
//
//    public String getMatchStr() {
//      return matchStr;
//    }
//
//    public void setEsFieldName(String esFieldName) {
//      this.esFieldName = esFieldName;
//    }
//
//    public void setMatchStr(String matchStr) {
//      this.matchStr = matchStr;
//    }
//
//    public String getResKeyName() {
//      return resKeyName;
//    }
//
//    public void setResKeyName(String resKeyName) {
//      this.resKeyName = resKeyName;
//    }
//  }
//
//  // json规则中key的类型
//  enum AnalysisValueRuleKeyEnum{
//    // 该key是字符串key，对应此次匹配结果
//    StringKey,
//
//    // 该key是对象的key，对应一个对象
//    ObjectKey,
//
//    // 该key是数组的key，对应一个数组
//    ArrKey;
//  }
//
//  class KeyStructure {
//    // key类型
//    private AnalysisValueRuleKeyEnum analysisValueRuleKeyEnum;
//
//    // key名
//    private String keyName;
//
//    public KeyStructure(String keyName, AnalysisValueRuleKeyEnum analysisValueRuleKeyEnum){
//      this.keyName = keyName;
//      this.analysisValueRuleKeyEnum = analysisValueRuleKeyEnum;
//
//    }
//
//    public AnalysisValueRuleKeyEnum getAnalysisValueRuleKeyEnum() {
//      return analysisValueRuleKeyEnum;
//    }
//
//    public String getKeyName() {
//      return keyName;
//    }
//
//    public void setAnalysisValueRuleKeyEnum(AnalysisValueRuleKeyEnum analysisValueRuleKeyEnum) {
//      this.analysisValueRuleKeyEnum = analysisValueRuleKeyEnum;
//    }
//
//    public void setKeyName(String keyName) {
//      this.keyName = keyName;
//    }
//  }
//}
