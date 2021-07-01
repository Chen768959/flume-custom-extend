package parsing_es_sink;

/**
 * @author Chen768959
 * @date 2021/7/6
 */
public class Constants {

  private static Constants constants = new Constants();

  private Constants(){}

  public static Constants getInstance(){
    return constants;
  }

  private final String eid = "eid";
  private final String etm = "etm";
  private final String phoneNum = "phoneNum";
  private final String imei = "imei";
  private final String imsi = "imsi";
  private final String androidID = "androidID";
  private final String idfa = "idfa";
  private final String oaid = "oaid";
  private final String udid = "udid";
  private final String ip = "ip";
  private final String devinfo = "devinfo";
  private final String event = "event";
  private final String channel = "channel";
  private final String appid = "appid";

  private String esIndexPre = "";

  public String getEsIndexPre() {
    return esIndexPre;
  }

  public void setEsIndexPre(String esIndexPre) {
    this.esIndexPre = esIndexPre;
  }

  public String getAndroidID() {
    return androidID;
  }

  public String getDevinfo() {
    return devinfo;
  }

  public String getEid() {
    return eid;
  }

  public String getEtm() {
    return etm;
  }

  public String getEvent() {
    return event;
  }

  public String getIdfa() {
    return idfa;
  }

  public String getImei() {
    return imei;
  }

  public String getImsi() {
    return imsi;
  }

  public String getIp() {
    return ip;
  }

  public String getOaid() {
    return oaid;
  }

  public String getPhoneNum() {
    return phoneNum;
  }

  public String getUdid() {
    return udid;
  }

  public String getChannel() {
    return channel;
  }

  public String getAppid() {
    return appid;
  }
}
