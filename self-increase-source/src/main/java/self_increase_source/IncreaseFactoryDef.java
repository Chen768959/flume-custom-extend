package self_increase_source;

import org.apache.flume.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author Chen768959
 * @date 2021/6/20
 */
public class IncreaseFactoryDef implements IncreaseFactory{
  private final Random random = new Random();

  @Override
  public List<Event> increaseEvents(int increaseNum) {
    List<Event> eventList = new ArrayList<>(increaseNum);

    for (int i=0; i<increaseNum; i++){
      Event event = null;
      switch (random.nextInt(2)){
        case 0:
          event = getAppEvent();
          break;
        case 1:
          event = getWebEvent();
          break;
      }

      eventList.add(event);
    }

    return eventList;
  }

  private Event getWebEvent() {
    return null;
  }

  private Event getAppEvent() {
    return null;
  }

  public static void main(String[] aaa){
    Random random = new Random();
    for (int i=0; i<100; i++){
      System.out.println(random.nextInt(2));
    }

  }
}
