package self_increase_source;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Chen768959
 * @date 2021/6/21
 */
public class RandomUtil {
  private static long nowTime = System.currentTimeMillis();

  private static final Random random = new Random();

  static {
    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        long nTime = System.currentTimeMillis();
        switch (random.nextInt(2)){
          case 0:
            nowTime = nTime+random.nextInt(100000);
            break;
          case 1:
            nowTime = nTime-random.nextInt(100000);
            break;
        }
      }
    },0, 1, TimeUnit.SECONDS);
  }

  public static String getUuid(){
    return UUID.randomUUID().toString();
  }

  public static String getTime(){
    return Long.toString(nowTime);
  }

  public static String getEKtype(){
    switch (random.nextInt(3)){
      case 0:
        return "60000000";
      case 1:
        return "56000004";
      case 2:
        return "event";
    }

    return "event";
  }
  public static String get65EID(){
    switch (random.nextInt(2)){
      case 0:
        return "60000000";
      case 1:
        return "56000004";
    }

    return "60000000";
  }

  public static String getPid(){
    return "TEST";
  }

  public static String getPageId(){
    return "TEST";
  }

  public static String getSubsessionServiceURL(){
    return "TEST";
  }

  public static String getMG_MSG_PROGRAM_URL(){
    return "TEST";
  }

  public static String getEVtype(){
    return "TEST";
  }

  public static String getGroupId(){
    return "TEST";
  }

  public static String getMgdbId(){
    return "TEST";
  }
}
