package Utils;

import java.io.IOException;
import java.util.*;

import com.mongodb.client.FindIterable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

public class MongoDBUtils {

	private static final Logger logger = Logger.getLogger(MongoDBUtils.class);
	private static final PropertiesUtils prop = new PropertiesUtils("conf/mongodb.properties", logger);
	private static String host = null;
	private static int port = 27017;
	//数据库名
	private static String databasename = null;

	//表名
	private static String collectionname = null;

	//表名
	private static String[] collectionnames = null;
	//分页条数
	private static int pagesize = 0;

	//连接到 mongodb 服务
	private static MongoClient mo = null;
	//连接到数据库
	private static MongoDatabase database = null;

	/**
	 * 作者: 王坤造
	 * 日期: 2016/12/21 18:51
	 * 名称：从properties获取mongodb集群配置和同步周期,并初始化mongodb和
	 * 备注：
	 */
	static {
		pagesize = prop.getPropertyWithInteger("pagesize");
		host = prop.getProperty("host");
		port = prop.getPropertyWithInteger("port");
		databasename = prop.getProperty("databasename");
		collectionnames = StringUtils.split(prop.getProperty("collectionnames"), ',');
		init();
	}

//	public static void insertDataToMongo(List<Document> documents,String collectionName) {
//		try {
//
//			MongoCollection<Document> collection = database.getCollection(collectionName);
//			collection.insertMany(documents);
//		} catch (Exception e) {
//			log.error("mongo batch insert fail", e);
//		}
//	}

//	// 查询所有数据
//	public static List<DBObject> queryAll(String collectionName) {
//		List<DBObject> rsJson = new ArrayList<DBObject>();
//		DBCollection users = MongoUtil.getDBConnectionWithoutAuth(collectionName);
//		// -1 表示倒序
//		DBCursor cur = users.find().sort(new BasicDBObject("date",-1));
//		System.out.println("数据总条数:" + users.count());
//		while (cur.hasNext()) {
//			rsJson.add(cur.next());
//		}
//		return rsJson;
//	}

	/**
	 * 作者: 王坤造
	 * 日期: 2017/1/16 10:39
	 * 名称：根据objectid判断是否存在
	 * 备注：
	 */
	public static boolean isExist(String id) {
		return isExist(collectionname, id);
	}

	/**
	 * 作者: 王坤造
	 * 日期: 2017/1/16 10:39
	 * 名称：根据objectid判断是否存在
	 * 备注：
	 */
	public static boolean isExist(String collectionName, String id) {
		if (StringUtils.isNotEmpty(id)) {
			MongoCollection<Document> collect = database.getCollection(collectionName);
			BasicDBObject query = new BasicDBObject("_id", new ObjectId(id));
			FindIterable<Document> documents = collect.find(query);
			return documents.first() != null;
		}
		return false;
	}

	/**
	 * 作者: 王坤造
	 * 日期: 2016/12/30 17:18
	 * 名称：带条件查询
	 * 备注：
	 */
	public static long getCount(String collectionName, Bson query) {
		List<Document> list = new ArrayList<Document>();
		MongoCollection<Document> collect = database.getCollection(collectionName);
		FindIterable<Document> iter;
		if (query == null) {
			iter = collect.find();
		} else {
			//BasicDBObject query = new BasicDBObject("date", new BasicDBObject("$gte", timeBegin).append("$lt", timeEnd));//3696
			iter = collect.find(query);
		}
		long count = 0l;
		for (Document document : iter) {
			count++;
		}
		return count;
	}

	/**
	 * 作者: 王坤造
	 * 日期: 2016/12/30 17:18
	 * 名称：带条件查询
	 * 备注：
	 */
	public static long getCount(Bson query) {
		return getCount(collectionname, query);
	}

	/**
	 * 作者: 王坤造
	 * 日期: 2016/12/30 17:18
	 * 名称：带条件查询
	 * 备注：
	 */
	public static List<Document> getList(Bson query, Bson sort, int pageIndex) {
		return getList(collectionname, query, sort, pageIndex);
	}

	/**
	 * 作者: 王坤造
	 * 日期: 2016/12/30 17:18
	 * 名称：带条件查询
	 * 备注：
	 */
	public static List<Document> getList(String collectionName, Bson query, Bson sort, int pageIndex) {
		List<Document> list = new ArrayList<Document>();
		MongoCollection<Document> collect = database.getCollection(collectionName);
		FindIterable<Document> iter;
		if (query == null) {
			iter = collect.find();
		} else {
			//BasicDBObject query = new BasicDBObject("date", new BasicDBObject("$gte", timeBegin).append("$lt", timeEnd));//3696
			iter = collect.find(query);
		}
		if (sort != null) {
			//BasicDBObject sort = new BasicDBObject("date", 1);//1升序;-1降序
			iter = iter.sort(sort);
		}
		if (pageIndex < 1) {
			pageIndex = 1;
		}
		iter = iter.skip((pageIndex - 1) * pagesize).limit(pagesize);
		for (Document document : iter) {
			list.add(document);
		}
		return list;
	}

	/**
	 * 作者: 王坤造
	 * 日期: 2016/12/30 17:18
	 * 名称：带条件查询
	 * 备注：
	 */
	public static List<Document> getList(String collectionName, Bson query, Bson sort, int pageIndex, int pageSize) {
		List<Document> list = new ArrayList<Document>(pageSize);
		MongoCollection<Document> collect = database.getCollection(collectionName);
		FindIterable<Document> iter;
		if (query == null) {
			iter = collect.find();
		} else {
			//BasicDBObject query = new BasicDBObject("date", new BasicDBObject("$gte", timeBegin).append("$lt", timeEnd));//3696
			iter = collect.find(query);
		}
		if (sort != null) {
			//BasicDBObject sort = new BasicDBObject("date", 1);//1升序;-1降序
			iter = iter.sort(sort);
		}
		if (pageIndex < 1) {
			pageIndex = 1;
		}
		iter = iter.skip((pageIndex - 1) * pageSize).limit(pageSize);
		for (Document document : iter) {
			list.add(document);
		}
		return list;
	}

	public static int getPagesize() {
		return pagesize;
	}

	/**
	 * 作者: 王坤造
	 * 日期: 2016/12/30 15:00
	 * 名称：初始化MongoDBClient和创建连接数据库
	 * 备注：
	 */
	private static void init() {
		if (mo == null) {
			mo = new MongoClient(host, port);
		}
		if (database == null) {
			database = mo.getDatabase(databasename);
		}
	}


	public static String[] getCollectionnames() {
		return collectionnames;
	}


	public static void main(String[] args) {
//        String index = "chinadrugtrials";
//        String type = "chinadrugtrials";
//
//        ArrayList<List<Document>> lists = new ArrayList<>();
//        ArrayList<String> jsons = new ArrayList<>();
//
////        BasicDBObject query = new BasicDBObject("date", new BasicDBObject("$gte", timeBegin).append("$lt", timeEnd));//3696
//        BasicDBObject query = null;//3697
//        int size = pagesize;
//        int pageIndex = 1;
//        int total = 0;
//        while (size == pagesize) {
//            List<Document> docums = MongoDBUtils.getList(collectionname, query, pageIndex++);//3697
//            size = docums.size();
//            total += size;
//            lists.add(docums);
//        }
//        for (List<Document> list : lists) {
//            for (Document docu : list) {
//                HashMap<String, Object> hm = new HashMap<>();
//                for (String key : docu.keySet()) {
//                    if (!key.equals("_id") && !key.equals("currentpage") && !key.equals("ckm_index") && !key.equals("ckm_id")) {
//                        hm.put(key, docu.get(key));
//                    }
//                }
//                jsons.add(esUtils.ObjectToJson(hm));
//            }
//        }
//
//        esUtils.operBulk(index, type, jsons);
//
//        System.out.println(total + ":" + pageIndex);


//		ObjectId("5865c1abc42a2611443f299e")
//		System.out.println(MongoDBUtils.isExist(collectionname, "5865c1abc42a2611443f299d"));
//		System.out.println(MongoDBUtils.isExist(collectionname, "5865c1abc42a2611443f299e"));


//		String path=("conf/mongodb.properties");
//		URL url = ClassLoader.getSystemResource(path);
//		System.out.println(url);


//		HashMap<String, String> map = new HashMap<String, String>() {{
//			put("id1", "aa");
//			put("date1", "bb");
//			put("id", "cc");
//		}};
//		PropertiesUtils prop = new PropertiesUtils("conf/mongodb.properties");
//		System.out.println(prop.getProperty("host"));
//		System.out.println(3);

//		isExist("587b6e3c5eec5619f474834f");


		int length = 2;
		BasicDBObject[] objects = new BasicDBObject[length];
		objects[0] = new BasicDBObject("currentTime", "N206910");
		objects[1] = new BasicDBObject("type", "通知");
		BasicDBObject query = new BasicDBObject();
		query.put("$and", objects);
		List<Document> list = MongoDBUtils.getList("news_detection", query, null, 1);
		if (CollectionUtils.isNotEmpty(list)) {
			Document foriDocum = list.get(0);
		}
	}
}
