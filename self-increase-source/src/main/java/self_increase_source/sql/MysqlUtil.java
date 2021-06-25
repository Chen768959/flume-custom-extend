package self_increase_source.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import self_increase_source.RandomUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Chen768959
 * @date 2021/6/25
 */
public class MysqlUtil {
  private static final Logger logger = LoggerFactory.getLogger(MysqlUtil.class);

  private static MysqlUtil mysqlUtil = new MysqlUtil();

  private Connection conn;

  private String nowTime;

  private String afterTime;

  private MysqlUtil(){
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        long time = Long.parseLong(RandomUtil.getTime());
        long aTime = time+36000000L;
        nowTime = simpleDateFormat.format(new Date(time));
        afterTime = simpleDateFormat.format(new Date(aTime));
      }
    },0, 1, TimeUnit.SECONDS);
  }

  public static MysqlUtil getInstance(){
    return mysqlUtil;
  }

  public void init(String mysqlHost, String mysqlAccount, String mysqlPassword, String mysqlDatabase){
    // 不同的数据库有不同的驱动
    String driverName = "com.mysql.jdbc.Driver";
    String url = "jdbc:mysql://"+mysqlHost+"/"+mysqlDatabase;
    String user = mysqlAccount;
    String password = mysqlPassword;

    try {
      // 加载驱动
      Class.forName(driverName);
      // 设置 配置数据
      this.conn = DriverManager.getConnection(url, user, password);
    } catch (ClassNotFoundException e) {
      logger.error("mysql初始化失败",e);
    } catch (SQLException e) {
      logger.error("mysql初始化失败",e);
    }
  }

  // 查询pid和mgdb_id
  private static final String selectPidAndMgdbId =
          "SELECT pid,mgdb_id FROM t_amber_program WHERE ((start_time >= ? AND start_time <= ? ) OR (end_time >= ? AND end_time <= ?)) AND pid IS NOT NULL AND mgdb_id IS NOT NULL AND pid != '' AND mgdb_id != ''";
  public String[][] selectPidAndMgdbId(){

    return executeQuery("SELECT pid,mgdb_id FROM t_amber_program WHERE ((start_time >= '" +nowTime+
            "' AND start_time <= '" +afterTime+
            "' ) OR (end_time >= '" +nowTime+
            "' AND end_time <= '" +afterTime+
            "')) AND pid IS NOT NULL AND mgdb_id IS NOT NULL AND pid != '' AND mgdb_id != ''",2);
  }

  private static final String selectPidAndMgdbIdAll =
          "SELECT pid,mgdb_id FROM t_amber_program where pid IS NOT NULL AND mgdb_id IS NOT NULL AND pid != '' AND mgdb_id != ''";
  public String[][] selectPidAndMgdbIdAll(){
    return executeQuery(selectPidAndMgdbIdAll,2);
  }

  private static final String selectPidAndMgdbIdByDaCarousel =
          "SELECT pid,mgdb_id FROM t_amber_program where pid IN (SELECT id FROM da_carousel_info) AND pid IS NOT NULL AND mgdb_id IS NOT NULL AND pid != '' AND mgdb_id != ''";
  public String[][] selectPidAndMgdbIdByDaCarousel(){
    return executeQuery(selectPidAndMgdbIdByDaCarousel,2);
  }

  private static final String selectGroupIdAndPageIdByageId =
          "SELECT group_id,group_location FROM dwd_dim_lego_group_1h_full_hourly where group_location in('73b78ab30193471e811fce017ccbfa37','1eb6923afaa84b4eb5fb58e8da5bf1c2','cefa371e9c5e43228f46a44125d8687f') AND group_id IS NOT NULL AND group_id != ''";
  public String[][] selectGroupIdAndPageIdByageId(){
    return executeQuery(selectGroupIdAndPageIdByageId,2);
  }

  private String[][] executeQuery(String sql, int resLineNum){
    String[][] res = {};
    PreparedStatement preparedStatement = null;
    ResultSet resultSet = null;
    try {
      preparedStatement = conn.prepareStatement(sql);

      resultSet = preparedStatement.executeQuery();
      int i = 0;
      if (resultSet!=null){
        resultSet.last();
        if (resultSet.getRow()>0){
          String[][] resultSetArr = new String[resultSet.getRow()][resLineNum];
          resultSet.beforeFirst();
          while (resultSet.next()){
            String[] row = new String[resLineNum];
            for (int j=1; j<=resLineNum; j++){
              row[j-1] = resultSet.getString(j);
            }
            resultSetArr[i] = row;
            i++;
          }

          res = resultSetArr;
        }
      }
    } catch (SQLException e) {
      logger.error("mysql查询失败",e);
    }finally {
      if (resultSet!=null){
        try {
          resultSet.close();
        } catch (SQLException e) {
          logger.error("resultSet关闭失败",e);
        }
      }

      if (preparedStatement!=null){
        try {
          preparedStatement.close();
        } catch (SQLException e) {
          logger.error("preparedStatement关闭失败",e);
        }
      }
    }

    return res;
  }
}
