package MyWork;

import Utils.DateUtils;
import Utils.MongoDBUtils;
import Utils.PropertiesUtils;
import com.mongodb.BasicDBObject;
import elasticsearch.api.ESUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MongoDBToES {
	public static void main(String[] args) {
		Timer timer = new Timer();
		TaskMongoDBToES task = new TaskMongoDBToES();
		long period;
		if (TaskMongoDBToES.getHour() > 1) {
			period = TaskMongoDBToES.getHour() * 3600 * 1000;
		} else {
			period = TaskMongoDBToES.getDay() * 24 * 3600 * 1000;
		}
//		period = 20000;
		timer.schedule(task, 1000, period);
	}
}

/**
 * 名称：王坤造
 * 时间：2016/12/30.
 * 名称：
 * 备注：
 */
class TaskMongoDBToES extends java.util.TimerTask {
	private static final Logger logger = Logger.getLogger(TaskMongoDBToES.class);
	//配置文件
	private static final PropertiesUtils prop = new PropertiesUtils("conf/mongodb.properties", logger);
	//定时时间:小时(只能选择一个)
	private static int hour = 0;
	//定时时间:天(只能选择一个)
	private static int day = 0;
	//根据定时时间计算的要同步的开始时间
	private static long timeBegin = 0l;
	//根据定时时间计算的要同步的结束时间
	private static long timeEnd = 0l;
	//同步的数据的保存路径
	private static String path;
	//同步到es的最后时间
	private static HashMap<String, Long> dateMap = new HashMap<>();
	//mongodb中时间字段名称
	private static String datefield;
	//排除的字段[不保存到es]
	private static String[] excludefields;
	//批量写入到es的条数
	private static int bulknum;
	//要读取mongodb的表
	private static final String[] collectionnames = MongoDBUtils.getCollectionnames();

	/**
	 * 作者: 王坤造
	 * 日期: 2016/12/21 18:51
	 * 名称：从properties获取mongodb集群配置和同步周期,并初始化mongodb和
	 * 备注：
	 */
	static {//这个static方法只执行一次!
		datefield = prop.getProperty("datefield");
		excludefields = prop.getProperty("excludefields").split(",");
		path = prop.getProperty("path");

		//获取同步时间周期
		try {
			hour = Math.abs(prop.getPropertyWithInteger("hour"));
			if (hour < 1) {
				day = Math.abs(prop.getPropertyWithInteger("day"));
			}
		} catch (Exception e) {
			day = 1;
		}
	}

	@Override
	public void run() {
		//获取date
		getSyncDate();
		//更新date到xml
		setSynchronizeData();
		//获取开始时间,结束时间
		getTime();

		writeOld();
//		writeNew();
//		writeNewNew();
	}

	private void getSyncDate() {
		String line;
		HashMap<String, String> hm = new HashMap<>();
		try {
			BufferedReader bufr = new BufferedReader(new FileReader(new File(path)));
			while ((line = bufr.readLine()) != null) {
				if (StringUtils.isNotEmpty(line) && line.charAt(0) != '#') {
					int index = line.indexOf("=");
					hm.put(line.substring(0, index), line.substring(index + 1));
				}
			}
			bufr.close();
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("读取同步文件" + path + "出错:{}", e);
		}
		for (String key : hm.keySet()) {
			String lastdates = hm.get(key);
			if (StringUtils.isNotEmpty(lastdates)) {
				String[] arr = lastdates.split(",");
				ArrayList<Long> list = new ArrayList<>(arr.length);
				for (String s : arr) {
					try {
						list.add(Long.parseLong(s));
					} catch (Exception e) {
						e.printStackTrace();
						logger.error("读取同步文件" + path + "转化为Long出错:{}", e);
					}
				}
				if (list.size() > 0) {
					Collections.sort(list);
					dateMap.put(key, list.get(list.size() - 1));
				} else {
					dateMap.put(key, 0l);
				}
			} else {
				dateMap.put(key, 0l);
			}
		}
	}

	private void writeOld() {
		for (String colName : collectionnames) {
			String type = colName.toLowerCase();
			String indexNew = type + "alias";
			final long start = System.currentTimeMillis();
			ArrayList<List<Document>> lists = new ArrayList<>();
			BasicDBObject query = null;//3708
			Long date = dateMap.get(colName);
			int queryType = 0;
			if (date < 1) {
				queryType = 1;
				query = new BasicDBObject(datefield, new BasicDBObject("$lt", timeEnd));
			} else if (date < timeBegin) {
				queryType = 2;
				query = new BasicDBObject(datefield, new BasicDBObject("$gt", date).append("$lt", timeEnd));
			} else {
				queryType = 3;
				query = new BasicDBObject(datefield, new BasicDBObject("$gte", timeBegin).append("$lt", timeEnd));
			}
//			System.err.println("查询条件:" + queryType + "\t" + colName + " date:" + date + "\ttimeBegin:" + timeBegin + "\ttimeEnd:" + timeEnd);
			logger.info("查询条件:" + queryType + "\t" + colName + " date:" + date + "\ttimeBegin:" + timeBegin + "\ttimeEnd:" + timeEnd + System.lineSeparator());

			BasicDBObject sort = new BasicDBObject(datefield, 1);//1:升序;-1降序
			final int size = MongoDBUtils.getPagesize();//每次分页获取大小
			int currentSize = size;//当前获取分页的大小
			int pageIndex = 1;
			int total = 0;
			//System.err.println(MongoDBUtils.getCount(colName, query));

			//如果不相等说明已经获取到最后一页了
			while (size == currentSize) {
				List<Document> docums = MongoDBUtils.getList(colName, query, sort, pageIndex++);//3697
				currentSize = docums.size();
				total += currentSize;
				lists.add(docums);
				if (lists.size() > 10) {
					writeOldBulk(lists, indexNew, type, colName);
					lists.clear();
				}
			}
			if (lists.size() > 0) {
				writeOldBulk(lists, indexNew, type, colName);
			}
			final long end = System.currentTimeMillis();
//			System.out.println("导入时间:" + (end - start) + "ms\t" + colName + " total:" + total + "\t总页数:" + (pageIndex - 1));
			logger.info("导入时间:" + (end - start) + "ms\t" + colName + " total:" + total + "\t总页数:" + (pageIndex - 1) + System.lineSeparator());
		}
	}

	private void writeOldBulk(ArrayList<List<Document>> lists, String index, String type, String colName) {
		ArrayList<String> jsons = new ArrayList<>();
		for (List<Document> list : lists) {
			for (Document docu : list) {
				HashMap<String, Object> hm = new HashMap<>();
				for (String key : docu.keySet()) {
					boolean flag = false;
					for (String exclude : excludefields) {
						if (key.equals(exclude)) {
							flag = true;
							break;
						}
					}
					if (!flag) {
						hm.put(key, docu.get(key));
					}
				}
				jsons.add(ESUtils.ObjectToJson(hm));
			}
		}
		if (jsons.size() > 0) {
			boolean flag = ESUtils.operBulk(index, type, jsons);
			if (flag) {
				int length = lists.size() - 1;
				Document last = lists.get(length).get(lists.get(length).size() - 1);
				if (last != null) {
					try {
						dateMap.put(colName, (long) last.get(datefield));
						setSynchronizeData();
					} catch (Exception e) {
						e.printStackTrace();
						logger.error(last.get(datefield) + "转化为long类型出错:{}", e);
					}
				}
			}
		}
	}

	public void writeNewNew() {
//		BasicDBObject query = null;//3708
//		if (date < 1) {
//			query = new BasicDBObject(datefield, new BasicDBObject("$lt", timeEnd));
//		} else if (date < timeBegin) {
//			query = new BasicDBObject(datefield, new BasicDBObject("$gt", date).append("$lt", timeEnd));
//		} else {
//			query = new BasicDBObject(datefield, new BasicDBObject("$gte", timeBegin).append("$lt", timeEnd));
//		}
//		//System.err.println("date:"+date+"\ttimeBegin:"+timeBegin+"\ttimeEnd:"+timeEnd);
//		BasicDBObject sort = new BasicDBObject(datefield, 1);//1:升序;-1降序
//		int size = MongoDBUtils.getPagesize();//每次分页获取大小
//		int currentSize = size;//当前获取分页的大小
//		int pageIndex = 1;
//		int total = 0;
//
//		//System.err.println(MongoDBUtils.getCount(query));
//		ExecutorService service = Executors.newFixedThreadPool(40);
//		StorageNew<Document> storage = new StorageNew<>();
//
//
//		multiProduceNew(service, collectionnames, type, storage);
//
//		//如果不相等说明已经获取到最后一页了
//		while (size == currentSize) {
//			List<Document> docums = MongoDBUtils.getList(query, sort, pageIndex++);//3697
//			currentSize = docums.size();
//			total += currentSize;
//			LinkedBlockingQueue<Document> list = new LinkedBlockingQueue<>();
//			list.addAll(docums);
//			storage.addSource(list);
//		}
//		storage.isAddSourceOK = true;
//
//		System.out.println(new Date() + "\ttotal:" + total + "\t总页数:" + (pageIndex - 1));
	}

	private void multiProduceNew(ExecutorService service, String index, String type, StorageNew s) {
		//ExecutorService service = Executors.newFixedThreadPool(10);
		//几个线程读
		ProducerWithMapNew pro = new ProducerWithMapNew(s, index, type, excludefields);
		service.submit(pro);
		ProducerWithMapNew pro2 = new ProducerWithMapNew(s, index, type, excludefields);
		service.submit(pro2);
//		ProducerWithMapNew pro3 = new ProducerWithMapNew(s,collectionnames, type, excludefields);
//		service.submit(pro3);
//		ProducerWithMapNew pro4 = new ProducerWithMapNew(s,collectionnames, type, excludefields);
//		service.submit(pro4);
//		ProducerWithMapNew pro5 = new ProducerWithMapNew(s,collectionnames, type, excludefields);
//		service.submit(pro5);
//		ProducerWithMapNew pro6 = new ProducerWithMapNew(s,collectionnames, type, excludefields);
//		service.submit(pro6);
//		ProducerWithMapNew pro7 = new ProducerWithMapNew(s,collectionnames, type, excludefields);
//		service.submit(pro7);
//		ProducerWithMapNew pro8 = new ProducerWithMapNew(s,collectionnames, type, excludefields);
//		service.submit(pro8);
//		ProducerWithMapNew pro9 = new ProducerWithMapNew(s,collectionnames, type, excludefields);
//		service.submit(pro9);
//		ProducerWithMapNew pro10 = new ProducerWithMapNew(s,collectionnames, type, excludefields);
//		service.submit(pro10);
//        ProducerWithMap pro11 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro11);
//        ProducerWithMap pro12 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro12);
//        ProducerWithMap pro13 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro13);
//        ProducerWithMap pro14 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro14);
//        ProducerWithMap pro15 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro15);
//        ProducerWithMap pro16 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro16);
//        ProducerWithMap pro17 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro17);
//        ProducerWithMap pro18 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro18);
//        ProducerWithMap pro19 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro19);
//        ProducerWithMap pro20 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro20);
	}

	public void writeNew() {
//		final long start = System.currentTimeMillis();
//		LinkedBlockingQueue<Document> lists = new LinkedBlockingQueue<>();
//		BasicDBObject query = null;//3708
//		if (date < 1) {
//			//query = new BasicDBObject(datefield, new BasicDBObject("$lt", timeEnd));
//		} else if (date < timeBegin) {
//			query = new BasicDBObject(datefield, new BasicDBObject("$gt", date).append("$lt", timeEnd));
//		} else {
//			query = new BasicDBObject(datefield, new BasicDBObject("$gte", timeBegin).append("$lt", timeEnd));
//		}
//		BasicDBObject sort = new BasicDBObject(datefield, 1);//1:升序;-1降序
//		int size = MongoDBUtils.getPagesize();//每次分页获取大小
//		int currentSize = size;//当前获取分页的大小
//		int pageIndex = 1;
//		int total = 0;
//		ExecutorService service = Executors.newFixedThreadPool(40);
//		Storage storage = new Storage();
//
//		boolean flag = false;
//		while (size == currentSize) {
//			List<Document> docums = MongoDBUtils.getList(query, sort, pageIndex++);//3697
//			currentSize = docums.size();
//			total += size;
//			lists.addAll(docums);
//			if (lists.size() >= 100000 && !flag) {
//				flag = true;
//				productAndConsume(service, storage, lists);
//			}
//		}
//		if (!flag) {
//			productAndConsume(service, storage, lists);
//		}
//		try {
//			service.shutdown();
//			if (!service.awaitTermination(1, TimeUnit.HOURS)) {
//				service.shutdownNow();
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//			service.shutdownNow();
//		}
//		final long end = System.currentTimeMillis();
//		System.out.println((end - start) + "ms" + "\ttotal:" + total);//29s
	}

	private void productAndConsume(ExecutorService service, Storage storage, LinkedBlockingQueue<Document> lists) {
		try {
//			multiProduce(service, collectionnames, type, lists, storage);
			multiConsume(service, storage);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void multiProduce(ExecutorService service, String index, String type, LinkedBlockingQueue<? extends Map> list, Storage s) throws Exception {
		//ExecutorService service = Executors.newFixedThreadPool(10);
		//几个线程读
		ProducerWithMap pro = new ProducerWithMap(index, type, excludefields, datefield, list, s);
		service.submit(pro);
		ProducerWithMap pro2 = new ProducerWithMap(index, type, excludefields, datefield, list, s);
		service.submit(pro2);
		ProducerWithMap pro3 = new ProducerWithMap(index, type, excludefields, datefield, list, s);
		service.submit(pro3);
		ProducerWithMap pro4 = new ProducerWithMap(index, type, excludefields, datefield, list, s);
		service.submit(pro4);
		ProducerWithMap pro5 = new ProducerWithMap(index, type, excludefields, datefield, list, s);
		service.submit(pro5);
		ProducerWithMap pro6 = new ProducerWithMap(index, type, excludefields, datefield, list, s);
		service.submit(pro6);
		ProducerWithMap pro7 = new ProducerWithMap(index, type, excludefields, datefield, list, s);
		service.submit(pro7);
		ProducerWithMap pro8 = new ProducerWithMap(index, type, excludefields, datefield, list, s);
		service.submit(pro8);
		ProducerWithMap pro9 = new ProducerWithMap(index, type, excludefields, datefield, list, s);
		service.submit(pro9);
		ProducerWithMap pro10 = new ProducerWithMap(index, type, excludefields, datefield, list, s);
		service.submit(pro10);
//        ProducerWithMap pro11 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro11);
//        ProducerWithMap pro12 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro12);
//        ProducerWithMap pro13 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro13);
//        ProducerWithMap pro14 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro14);
//        ProducerWithMap pro15 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro15);
//        ProducerWithMap pro16 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro16);
//        ProducerWithMap pro17 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro17);
//        ProducerWithMap pro18 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro18);
//        ProducerWithMap pro19 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro19);
//        ProducerWithMap pro20 = new ProducerWithMap(collectionnames, type, idName,excludefields, list, s);
//        service.submit(pro20);
	}

	private void multiConsume(ExecutorService service, Storage s) {
		ConsumerWithMap con = new ConsumerWithMap(s, bulknum, "con", path);
		service.submit(con);
		ConsumerWithMap con2 = new ConsumerWithMap(s, bulknum, "con2", path);
		service.submit(con2);
		ConsumerWithMap con3 = new ConsumerWithMap(s, bulknum, "con3", path);
		service.submit(con3);
		ConsumerWithMap con4 = new ConsumerWithMap(s, bulknum, "con4", path);
		service.submit(con4);
		ConsumerWithMap con5 = new ConsumerWithMap(s, bulknum, "con5", path);
		service.submit(con5);
		ConsumerWithMap con6 = new ConsumerWithMap(s, bulknum, "con6", path);
		service.submit(con6);
		ConsumerWithMap con7 = new ConsumerWithMap(s, bulknum, "con7", path);
		service.submit(con7);
		ConsumerWithMap con8 = new ConsumerWithMap(s, bulknum, "con8", path);
		service.submit(con8);
		ConsumerWithMap con9 = new ConsumerWithMap(s, bulknum, "con9", path);
		service.submit(con9);
		ConsumerWithMap con10 = new ConsumerWithMap(s, bulknum, "con10", path);
		service.submit(con10);
//        ConsumerWithMap con11 = new ConsumerWithMap( s, num, "con11");
//        service.submit(con11);
//        ConsumerWithMap con12 = new ConsumerWithMap( s, num, "con12");
//        service.submit(con12);
//        ConsumerWithMap con13 = new ConsumerWithMap( s, num, "con13");
//        service.submit(con13);
//        ConsumerWithMap con14 = new ConsumerWithMap( s, num, "con14");
//        service.submit(con14);
//        ConsumerWithMap con15 = new ConsumerWithMap( s, num, "con15");
//        service.submit(con15);
//        ConsumerWithMap con16 = new ConsumerWithMap( s, num, "con16");
//        service.submit(con16);
//        ConsumerWithMap con17 = new ConsumerWithMap( s, num, "con17");
//        service.submit(con17);
//        ConsumerWithMap con18 = new ConsumerWithMap( s, num, "con18");
//        service.submit(con18);
//        ConsumerWithMap con19 = new ConsumerWithMap( s, num, "con19");
//        service.submit(con19);
//        ConsumerWithMap con20 = new ConsumerWithMap( s, num, "con20");
//        service.submit(con20);
	}


	public static int getHour() {
		return hour;
	}

	public static int getDay() {
		return day;
	}

	/**
	 * 作者: 王坤造
	 * 日期: 2016/12/30 14:11
	 * 名称：获取开始结束时间
	 * 备注：
	 */
	private void getTime() {
		if (hour > 0) {
			//获取前几小时之前的数据
			timeBegin = DateUtils.getAroundTime(new Date(System.currentTimeMillis()), -hour).getTime();
			timeEnd = DateUtils.getAroundTime(new Date(System.currentTimeMillis()), 0).getTime();
		} else {
			//获取前几天之前的数据
			timeBegin = DateUtils.getAroundDay(new Date(System.currentTimeMillis()), -day).getTime();
			timeEnd = DateUtils.getAroundDay(new Date(System.currentTimeMillis()), 0).getTime();
		}
	}

	private void setSynchronizeData() {
		try {
			BufferedWriter bufw = new BufferedWriter(new FileWriter(path));
			for (String key : dateMap.keySet()) {
				bufw.write(key + "=" + dateMap.get(key) + "," + System.lineSeparator());
			}
			bufw.flush();
			bufw.close();
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("map写入到" + path + "文件出错!{}", e);
		}
	}

}

