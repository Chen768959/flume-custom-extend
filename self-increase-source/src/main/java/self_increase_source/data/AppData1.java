package self_increase_source.data;

import self_increase_source.RandomUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Chen768959
 * @date 2021/6/22
 */
public class AppData1 extends BaseData implements Data{

  private final HashMap<String, Object> appData;
  private final Map.Entry<String, Object> entryDI;
  private final Map.Entry<String, Object> entryUDID;
  private final Map.Entry<String, Object> entryEvents_OCID;
  private final Map.Entry<String, Object> entryEvents_ETM;
  private final Map.Entry<String, Object> entryEvents_DTD_type;
  private final Map.Entry<String, Object> entryEvents_DTD_pageID;
  private final Map.Entry<String, Object> entryEvents_DTD_SubsessionServiceURL;
  private final Map.Entry<String, Object> entryEvents_DTD_MG_MSG_TIME;
  private final Map.Entry<String, Object> entryEvents_DTD_Session;
  private final Map.Entry<String, Object> entryEvents_DTD_contentId;
  private final Map.Entry<String, Object> entryEvents_DTD_MG_MSG_PROGRAM_URL;

  public AppData1(){
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
          put("EID","kesheng_quality");
          put("OCID","ceshieOCID");
          put("ETM","1624236617392");
          put("TDT",new ArrayList<Map<String,Object>>(){{
            add(new HashMap<String,Object>(){{
              put("EK","type");
              put("EV","event");
            }});
            add(new HashMap<String,Object>(){{
              put("EK","pageID");
              put("EV","123456789");
            }});
            add(new HashMap<String,Object>(){{
              put("EK","SubsessionServiceURL");
              put("EV","www.ceshi.ceom");
            }});
            add(new HashMap<String,Object>(){{
              put("EK","MG_MSG_TIME");
              put("EV","1624242273432");
            }});
            add(new HashMap<String,Object>(){{
              put("EK","Session");
              put("EV","CESHI");
            }});
            add(new HashMap<String,Object>(){{
              put("EK","contentId");
              put("EV","123456789");
            }});
            add(new HashMap<String,Object>(){{
              put("EK","MG_MSG_PROGRAM_URL");
              put("EV","www.ceshi.ceom");
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

    ArrayList<Map<String, Object>> tdt = getArrByKey(eventsMap, "TDT");
    this.entryEvents_DTD_type = getEntryByEK(tdt, "type");
    this.entryEvents_DTD_pageID = getEntryByEK(tdt, "pageID");
    this.entryEvents_DTD_SubsessionServiceURL = getEntryByEK(tdt, "SubsessionServiceURL");
    this.entryEvents_DTD_MG_MSG_TIME = getEntryByEK(tdt, "MG_MSG_TIME");
    this.entryEvents_DTD_Session = getEntryByEK(tdt, "Session");
    this.entryEvents_DTD_contentId = getEntryByEK(tdt, "contentId");
    this.entryEvents_DTD_MG_MSG_PROGRAM_URL = getEntryByEK(tdt, "MG_MSG_PROGRAM_URL");
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
    this.entryEvents_DTD_type.setValue(RandomUtil.getEKtype());
    this.entryEvents_DTD_pageID.setValue(RandomUtil.getPageId());
    this.entryEvents_DTD_SubsessionServiceURL.setValue(RandomUtil.getSubsessionServiceURL());
    this.entryEvents_DTD_MG_MSG_TIME.setValue(RandomUtil.getTime());
    this.entryEvents_DTD_Session.setValue(RandomUtil.getUuid());
    this.entryEvents_DTD_contentId.setValue(RandomUtil.getPid());
    this.entryEvents_DTD_MG_MSG_PROGRAM_URL.setValue(RandomUtil.getMG_MSG_PROGRAM_URL());
  }
}
