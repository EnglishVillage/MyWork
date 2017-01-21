package Utils;

import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.io.InputStream;
import java.util.*;

/**
 * 名称：王坤造
 * 时间：2016/12/29.
 * 名称：
 * 备注：
 */
public class MongodbUtilsOld {

    private static String host = null;
    private static int port = 27017;
    //数据库名
    private static String dbname = null;
    private static String collectionname = null;
    //查询过滤的时间范围
    private static long timeBegin = 0l;
    private static long timeEnd = 0l;
    private static int hour = 0;
    private static int day = 0;

    /**
     * 作者: 王坤造
     * 日期: 2016/12/21 18:51
     * 名称：从properties获取es集群配置
     * 备注：
     */
    static {
        Properties prop = new Properties();
        try (InputStream inputStream = ClassLoader.getSystemResourceAsStream("conf/mongodb.properties");) {
            prop.load(inputStream);
            host = prop.getProperty("host");
            port = Integer.parseInt(prop.getProperty("port"));
            dbname = prop.getProperty("dbname");
            collectionname = prop.getProperty("collectionname");

            //获取同步时间周期
            try {
                hour = Math.abs(Integer.parseInt(prop.getProperty("hour")));
                if (hour < 1) {
                    day = Math.abs(Integer.parseInt(prop.getProperty("day")));
                }
            } catch (Exception e) {
                day = 1;
            }
            //获取开始时间,结束时间
            getTime();


            timeBegin = 1483063510708l;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //mongo连接
    private static MongoClient mo = new MongoClient("192.168.1.136", 27017);
    //数据库
    private static MongoDatabase database = mo.getDatabase("spider");
    //集合

    public MongodbUtilsOld() {
        //createInstence();
    }

    public MongodbUtilsOld(String host, int port, String dbName) {
        host = host;
        port = port;
        dbname = dbName;
        createInstence();
    }

    public Mongo createInstence() {
        return createInstence(host, port);
    }

    /**
     * 创建对象
     *
     * @param host
     * @param port
     * @return
     */
    public Mongo createInstence(String host, int port) {
        if (mo == null) {
            mo = new MongoClient(host, port);
        }
        return mo;
    }


//    /**
//     * 创建集合
//     *
//     * @param collName
//     * @return
//     */
//    @Deprecated
//    public boolean createColl(String collName) {
//        boolean flag = false;
//        if (database == null) {
//            connDb();
//        }
//        if (db.collectionExists(collName)) {
//            System.out.println("集合已经存在！");
//            return flag;
//        } else {
//            try {
//                db.createCollection(collName, null);
//                flag = true;
//            } catch (Exception e) {
//                System.out.println("集合创建失败：异常信息：" + e.getMessage());
//                return flag;
//            } finally {
//                closeMongo();
//            }
//
//        }
//        return flag;
//    }
//
//    /**
//     * 新增 单个
//     *
//     * @param collName
//     * @param obj
//     * @return
//     */
//    @Deprecated
//    public boolean addDbobject(String collName, BasicDBObject obj) {
//        DBCollection coll = getColl(collName);
//        try {
//            coll.insert(obj, WriteConcern.W1);
//            return true;
//        } catch (Exception e) {
//            return false;
//        } finally {
//            closeMongo();
//        }
//    }
//
//    /**
//     * 新增多个
//     *
//     * @param collName
//     * @param objlist
//     * @return
//     */
//    @Deprecated
//    public boolean addDbobject(String collName, List<DBObject> objlist) {
//        DBCollection coll = getColl(collName);
//        try {
//            coll.insert(objlist);
//            return true;
//        } catch (Exception e) {
//            return false;
//        } finally {
//            closeMongo();
//        }
//    }
//
//    /**
//     * 删除操作
//     *
//     * @param collName
//     * @param bobj
//     * @return
//     */
//    @Deprecated
//    public boolean rvdbobj(String collName, BasicDBObject bobj) {
//        DBCollection coll = getColl(collName);
//        try {
//            coll.remove(bobj);
//            return true;
//        } catch (Exception e) {
//            System.out.println("删除失败!" + e.getMessage());
//            return false;
//        } finally {
//            closeMongo();
//        }
//    }
//
//    /**
//     * 修改信息
//     *
//     * @param collName
//     * @param query
//     * @param o
//     * @return
//     */
//    @Deprecated
//    public boolean upbobj(String collName, BasicDBObject query, BasicDBObject o) {
//        DBCollection coll = getColl(collName);
//
//        try {
//            coll.update(query, o);
//            return true;
//        } catch (Exception e) {
//            System.out.println("更新修改操作失败！" + e.getMessage());
//            return false;
//        } finally {
//            closeMongo();
//        }
//
//    }

//    /**
//     * 查寻单个记录
//     *
//     * @param collName
//     * @param bobj
//     * @return
//     */
//    public DBObject getdbobj(String collName, BasicDBObject bobj) {
//        MongoCollection<HashMap> coll = getColl(collName);
//        try {
//
//            DBObject ob = coll.findOne(bobj);
//
//            return ob;
//        } catch (Exception e) {
//            System.out.println("查询单条失败！" + e.getMessage());
//            return null;
//        } finally {
//            closeMongo();
//        }
//    }

    /**
     * 得到集合
     *
     * @param collName
     * @return
     */
    public MongoCollection<Document> getColl(String collName) {
        if (database == null) {
            connDb();
        }
        MongoCollection<Document> dbColl = database.getCollection(collName);
        return dbColl;
    }

    /**
     * 查询多条
     *
     * @param collName
     * @return
     */
    public ArrayList<Document> getlistByTime(String collName) {
        ArrayList<Document> list = new ArrayList<>();
        try {
            MongoCollection<Document> coll = getColl(collName);
            BasicDBObject query = new BasicDBObject("date", new BasicDBObject("$gte", timeBegin).append("$lt", timeEnd));
            FindIterable<Document> cursor = coll.find(query);
            for (Document hm : cursor) {
                list.add(hm);
            }
            return list;
        } catch (Exception e) {
            System.out.println("查询出错了！" + e.getMessage());
            return null;
        } finally {
            closeMongo();
        }
    }

    //关闭连接
    public void closeMongo() {
        mo.close();
        mo = null;
    }


    // 带条件查询
    public static List<DBObject> queryWithCondition(String collectionName, DBObject condition) {
        List<DBObject> rsJson = new ArrayList<DBObject>();
        DB db = mo.getDB("spider");
        DBCollection users = db.getCollection(collectionName);
        DBCursor rs = users.find(condition);
        while (rs.hasNext()) {
            DBObject obj = rs.next();
            rsJson.add(obj);
        }
        return rsJson;

    }


    /*
     * 连接数据库
     */
    private MongoDatabase connDb() {
        if (mo == null) {
            createInstence();
        } else {
            if (database == null) {
                database = mo.getDatabase(dbname);
            }
        }
        return database;
    }

    /**
     * 作者: 王坤造
     * 日期: 2016/12/30 14:11
     * 名称：获取开始结束时间
     * 备注：
     */
    private static void getTime() {
        if (hour > 0) {
            //获取前几小时之前的数据
            timeBegin = getAroundTime(new Date(System.currentTimeMillis()), -hour).getTime();
            timeEnd = getAroundTime(new Date(System.currentTimeMillis()), 0).getTime();
        } else {
            //获取前几天之前的数据
            timeBegin = getAroundDay(new Date(System.currentTimeMillis()), -day).getTime();
            timeEnd = getAroundDay(new Date(System.currentTimeMillis()), 0).getTime();
        }
    }

    /**
     * 作者: 王坤造
     * 日期: 2016/12/30 11:19
     * 名称：获取今天周围的日期
     * 备注：
     * num:-1表示昨天,-2表示前天,-3表示前2天
     * 1表示明天,2表示后天
     */
    private static Date getAroundDay(Date date, int num) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, num);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        date = calendar.getTime();
        return date;
    }

    /**
     * 作者: 王坤造
     * 日期: 2016/12/30 11:19
     * 名称：获取今天周围的时间
     * 备注：
     * num:-1表示昨天,-2表示前天,-3表示前2天
     * 1表示明天,2表示后天
     */
    private static Date getAroundTime(Date date, int num) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.HOUR_OF_DAY, num);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        date = calendar.getTime();
        return date;
    }


    public static void main(String[] args) {
//        Date date = new Date(System.currentTimeMillis());
//        System.out.println(date);
//        Date nextDay = getAroundDay(date, -2);
//        System.out.println(nextDay.getTime());
//        System.out.println(nextDay);
//
//        System.out.println(timeBegin);
//        System.out.println(timeEnd);

        MongodbUtilsOld mongodbUtils = new MongodbUtilsOld();
        ArrayList<Document> hashMaps = mongodbUtils.getlistByTime(collectionname);


        System.out.println(1);
    }
}
