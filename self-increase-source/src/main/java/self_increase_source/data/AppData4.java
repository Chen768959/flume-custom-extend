package self_increase_source.data;

import self_increase_source.RandomUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Chen768959
 * @date 2021/6/22
 */
public class AppData4 extends BaseData implements Data{

  private final HashMap<String, Object> appData;
  private final Map.Entry<String, Object> entryDI;
  private final Map.Entry<String, Object> entryUDID;
  private final Map.Entry<String, Object> entryEvents_OCID;
  private final Map.Entry<String, Object> entryEvents_ETM;
  private final Map.Entry<String, Object> entryEvents_EID;
  private final Map.Entry<String, Object> entryEvents_TDT_account;
  private final Map.Entry<String, Object> entryEvents_TDT_SubsessionServiceURL;
  private final Map.Entry<String, Object> entryEvents_TDT_type;
  private final Map.Entry<String, Object> entryEvents_TDT_MG_MSG_PROGRAM_URL;
  private final Map.Entry<String, Object> entryEvents_TDT_contentId;
  private final Map.Entry<String, Object> entryEvents_TDT_MG_MSG_TIME;

  public AppData4(){
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
          put("EID","000000000");
          put("OCID","ceshieOCID");
          put("ETM","1624236617392");
          put("TDT",new ArrayList<Map<String,Object>>(){{
            add(new HashMap<String,Object>(){{
              put("EK","account");
              put("EV","account");
            }});
            add(new HashMap<String,Object>(){{
              put("EK","SubsessionServiceURL");
              put("EV","WWW.TEST.COM");
            }});
            add(new HashMap<String,Object>(){{
              put("EK","type");
              put("EV","60000000");
            }});
            add(new HashMap<String,Object>(){{
              put("EK","MG_MSG_PROGRAM_URL");
              put("EV","WWW.TEST.COM");
            }});
            add(new HashMap<String,Object>(){{
              put("EK","contentId");
              put("EV","111111111");
            }});
            add(new HashMap<String,Object>(){{
              put("EK","MG_MSG_TIME");
              put("EV","1624018567700");
            }});
          }});
        }});
      }});
    }};

    this.entryDI = getEntryByKey(appData,"DI");
    this.entryUDID = getEntryByKey(appData,"UDID");

    Map<String, Object> eventsMap = getArrByKey(appData, "Events").get(0);
    this.entryEvents_OCID = getEntryByKey(eventsMap,"OCID");
    this.entryEvents_ETM = getEntryByKey(eventsMap,"ETM");
    this.entryEvents_EID = getEntryByKey(eventsMap,"EID");

    ArrayList<Map<String, Object>> tdt = getArrByKey(eventsMap, "TDT");
    this.entryEvents_TDT_account = getEntryByEK(tdt,"account");
    this.entryEvents_TDT_SubsessionServiceURL = getEntryByEK(tdt,"SubsessionServiceURL");
    this.entryEvents_TDT_type = getEntryByEK(tdt,"type");
    this.entryEvents_TDT_MG_MSG_PROGRAM_URL = getEntryByEK(tdt,"MG_MSG_PROGRAM_URL");
    this.entryEvents_TDT_contentId = getEntryByEK(tdt,"contentId");
    this.entryEvents_TDT_MG_MSG_TIME = getEntryByEK(tdt,"MG_MSG_TIME");
  }

  @Override
  public Map<String, Object> getData() {
    return this.appData;
  }

  @Override
  public void makingRandom() {
    String uuid = RandomUtil.getUuid();
    this.entryDI.setValue(uuid);
    this.entryUDID.setValue(uuid);
    this.entryEvents_OCID.setValue(uuid);
    this.entryEvents_ETM.setValue(RandomUtil.getTime());
    this.entryEvents_EID.setValue(RandomUtil.get65EID());
    this.entryEvents_TDT_account.setValue(uuid);
    this.entryEvents_TDT_SubsessionServiceURL.setValue(RandomUtil.getSubsessionServiceURL());
    this.entryEvents_TDT_type.setValue(RandomUtil.getEKtype());
    this.entryEvents_TDT_MG_MSG_PROGRAM_URL.setValue(RandomUtil.getMG_MSG_PROGRAM_URL());
    this.entryEvents_TDT_contentId.setValue(RandomUtil.getPid());
    this.entryEvents_TDT_MG_MSG_TIME.setValue(RandomUtil.getTime());
  }
}
