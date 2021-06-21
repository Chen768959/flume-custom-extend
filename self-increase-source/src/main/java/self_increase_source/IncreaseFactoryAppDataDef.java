package self_increase_source;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flume.Event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Chen768959
 * @date 2021/6/21
 */
public class IncreaseFactoryAppDataDef implements IncreaseFactory{
  @Override
  public List<Event> increaseEvents(int increaseNum) throws JsonProcessingException {
    return null;
  }

  private Map<String, Object> getAppDataMain() {
    return new HashMap<String,Object>(){{
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
          put("EID","ceshiEID");
          put("OCID","ceshieOCID");
          put("ETM","1624236617392");
          put("TDT",new ArrayList<Map<String,Object>>(){{
            add(new HashMap<String,Object>(){{
              put("EK","data");
              put("EV",new HashMap<String,Object>(){{
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

            add(new HashMap<String,Object>(){{
              put("EK","SubsessionServiceURL");
              put("EV","http://www.bbbbb.com:80/");
            }});

            add(new HashMap<String,Object>(){{
              put("EK","type");
              put("EV","11111111111");
            }});

            add(new HashMap<String,Object>(){{
              put("EK","Session");
              put("EV","EWFWDEDW2343423523454");
            }});

            add(new HashMap<String,Object>(){{
              put("EK","MG_MSG_PROGRAM_URL");
              put("EV","http://WWW.MMMMMMMM.com/");
            }});
          }});
        }});
      }});
    }};
  }
}
