package self_increase_source.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import self_increase_source.RandomUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Chen768959
 * @date 2021/6/22
 */
public class AppData3 extends BaseData implements Data{

  private final HashMap<String, Object> appData;
  private final Map.Entry<String, Object> entryDI;
  private final Map.Entry<String, Object> entryUDID;
  private final Map.Entry<String, Object> entryEvents_OCID;
  private final Map.Entry<String, Object> entryEvents_ETM;
  private final Map.Entry<String, Object> entryEvents_TDT_type;
  private final Map.Entry<String, Object> entryEvents_TDT_data;
  private final Map.Entry<String, Object> entryEvents_TDT_data_sdkSessionInfo_udid;
  private final Map.Entry<String, Object> entryEvents_TDT_data_sdkSessionInfo_account;
  private final Map.Entry<String, Object> entryEvents_TDT_data_customEvent_type;
  private final Map.Entry<String, Object> entryEvents_TDT_data_customEvent_params_contentId;
  private final Map.Entry<String, Object> entryEvents_TDT_data_customEvent_params_pageID;
  private final Map.Entry<String, Object> entryEvents_TDT_data_customEvent_params_groupId;
  private final Map.Entry<String, Object> entryEvents_TDT_data_customEvent_params_extra_mgdbId;
  private final Map<String, Object> mapEvents_TDT_data;

  public AppData3(){
    this.appData = new HashMap<String,Object>(){{
      put("DevInfo",new HashMap<String,String>(){{
        put("IMEI1","12312313241234");
        put("LP","1");
        put("DI","D123456789-123456789-123456789");
        put("UDID","U123456789-123456789-123456789");
        put("DM","MI 5X");
        put("CPU","arm64-v8a");
        put("PROV","中国");
        put("UA","Android10");
        put("SC","1920x1080");
        put("IPLAT","36.894402");
        put("COUNTRY","中国");
        put("SDKV","1.2.3");
        put("SN","Android");
        put("IMSI1","222111555666777");
        put("AC","1116921");
        put("MO","11111");
        put("SV","10.1.0");
        put("APILevel","30");
        put("APPID","6");
        put("IP","2001:0db8:3c4d:0015:0000:0000:1a2f:1a2b");
        put("ISP","电信");
        put("AK","11137222eb2333aa90f8555tt2381111");
        put("AN","测试");
        put("CLIENTID","11112222333379");
        put("AVC","1.2.3|222");
        put("CPURATE","-1");
        put("IPLOC","111.222");
        put("MEMRATE","0.1111");
        put("CLIENTIPV6","2001:0db8:3c4d:0015:0000:0000:1a2f:1a2b");
        put("DB","xiaomi");
      }});

      put("Events",new ArrayList<Map<String,Object>>(){{
        add(new HashMap<String,Object>(){{
          put("EID","crystal_data");
          put("OCID","ceshieOCID");
          put("ETM","1624236617392");
          put("TDT",new ArrayList<Map<String,Object>>(){{
            add(new HashMap<String,Object>(){{
              put("EK","type");
              put("EV","event");
            }});
            add(new HashMap<String,Object>(){{
              put("EK","data");
              put("EV",new HashMap<String,Object>(){  {
                put("sdkSessionInfo",new HashMap<String,Object>(){{
                  put("udid","randomUdid");
                  put("account","randomAccount");
                }});
                put("customEvent",new ArrayList<Map<String,Object>>(){{
                  add(new HashMap<String,Object>(){{
                    put("eventName","native_action");
                    put("eventParams",new HashMap<String,Object>(){{
                      put("action",new HashMap<String,Object>(){{
                        put("type","XXXXXXXX_TYPE");
                        put("params",new HashMap<String,Object>(){{
                          put("extra",new HashMap<String,Object>(){{
                            put("mgdbId","100002222");
                          }});
                          put("contentId","100002222");
                          put("pageID","121313251234213432412revfdvdsfs");
                          put("groupId","121313251234213432412revfdvdsfs");
                        }});
                      }});
                    }});
                  }});
                }});
              }});
            }});
          }});
        }});
      }});
    }};

    HashMap<String,Object> devInfo = (HashMap<String,Object>)appData.get("DevInfo");
    this.entryDI = getEntryByKey(devInfo,"DI");
    this.entryUDID = getEntryByKey(devInfo,"UDID");

    Map<String, Object> eventsMap = getArrByKey(appData, "Events").get(0);
    this.entryEvents_OCID = getEntryByKey(eventsMap,"OCID");
    this.entryEvents_ETM = getEntryByKey(eventsMap,"ETM");

    ArrayList<Map<String, Object>> tdt = getArrByKey(eventsMap, "TDT");
    this.entryEvents_TDT_type = getEntryByEK(tdt, "type");
    this.entryEvents_TDT_data = getEntryByEK(tdt, "data");

    this.mapEvents_TDT_data = (Map<String, Object>) getEntryByEK(tdt, "data").getValue();
    this.entryEvents_TDT_data_sdkSessionInfo_udid = getEntryByKey((Map<String, Object>)mapEvents_TDT_data.get("sdkSessionInfo"), "udid");
    this.entryEvents_TDT_data_sdkSessionInfo_account = getEntryByKey((Map<String, Object>)mapEvents_TDT_data.get("sdkSessionInfo"), "account");
    Map<String, Object> action = (Map<String, Object>)
                                          ((Map<String, Object>)
                                              ((ArrayList<Map<String, Object>>) mapEvents_TDT_data.get("customEvent"))
                                                      .get(0).get("eventParams"))
                                                            .get("action");
    this.entryEvents_TDT_data_customEvent_type = getEntryByKey(action,"type");
    Map<String, Object> params = (Map<String, Object>)action.get("params");
    this.entryEvents_TDT_data_customEvent_params_contentId = getEntryByKey(params,"contentId");
    this.entryEvents_TDT_data_customEvent_params_pageID = getEntryByKey(params,"pageID");
    this.entryEvents_TDT_data_customEvent_params_groupId = getEntryByKey(params,"groupId");
    this.entryEvents_TDT_data_customEvent_params_extra_mgdbId = getEntryByKey((Map<String, Object>)params.get("extra"),"mgdbId");
  }

  @Override
  public Map<String, Object> getData() {
    return this.appData;
  }

  @Override
  public void makingRandom() throws JsonProcessingException {
    String uuid = RandomUtil.getUuid();
    this.entryDI.setValue(uuid);
    this.entryUDID.setValue(uuid);
    this.entryEvents_OCID.setValue(uuid);
    this.entryEvents_ETM.setValue(RandomUtil.getTime());
    this.entryEvents_TDT_type.setValue(RandomUtil.getEKtype());
    this.entryEvents_TDT_data_sdkSessionInfo_udid.setValue(uuid);
    this.entryEvents_TDT_data_sdkSessionInfo_account.setValue(uuid);
    this.entryEvents_TDT_data_customEvent_type.setValue(RandomUtil.getEVtype());
    this.entryEvents_TDT_data_customEvent_params_contentId.setValue(RandomUtil.getPid());
    this.entryEvents_TDT_data_customEvent_params_pageID.setValue(RandomUtil.getPageId());
    this.entryEvents_TDT_data_customEvent_params_groupId.setValue(RandomUtil.getGroupId());
    this.entryEvents_TDT_data_customEvent_params_extra_mgdbId.setValue(RandomUtil.getMgdbId());

    this.entryEvents_TDT_data.setValue(objectMapper.writeValueAsString(this.mapEvents_TDT_data));
  }
}
