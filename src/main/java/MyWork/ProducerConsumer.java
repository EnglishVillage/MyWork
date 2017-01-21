package MyWork;

import elasticsearch.api.ESUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 名称：王坤造
 * 时间：2016/12/9.
 * 名称：
 * 备注：
 */
public class ProducerConsumer {

}

/**
 * 作者: 王坤造
 * 日期: 2017/1/3 17:25
 * 名称：消费者,消费的产品是json串且含有id
 * 备注：
 * bulkNum:匹量提交数量
 */
class ConsumerWithJsonId implements Runnable {
	private Storage storage;
	private String name;
	private Integer bulkNum;

	public ConsumerWithJsonId(Storage storage, Integer bulkNum, String name) {
		this.storage = storage;
		this.name = name;
		this.bulkNum = bulkNum;
	}

	public void run() {
		String index = null;
		String type = null;
		ArrayList<String> jsons = new ArrayList<>(bulkNum);
		ArrayList<String> ids = new ArrayList<>(bulkNum);
		//这里一定设置为true,不解释!!!
		while (true) {
			jsons.clear();
			ids.clear();
			try {
				Product product = storage.pop();
				//第一次消费是时候,可能还没生产好,所以等待生产完成
				while (product == null) {
//                    System.out.println(name + ":product is null");
					//产品消费完成且生产者已经生产完成,则结束消费.
					if (storage.isOK) {
						System.out.println(name + ":storage.isOK");
						break;
					}
					Thread.sleep(100);
					continue;
				}
				if (product != null) {
					index = product.index;
					type = product.type;
					jsons.add(product.json);
					ids.add(product.id);
					for (int i = 1; i < 7000; i++) {
						product = storage.pop();
						if (product == null) {
							System.out.println(name + ":product is null");
							if (storage.isOK) {
								System.out.println(name + ":storage.isOK");
								break;
							}
							Thread.sleep(100);
							continue;
						}
						jsons.add(product.json);
						ids.add(product.id);
					}
					ESUtils.operBulk(index, type, jsons, ids);
				}
				//System.out.println(name + " : "+storage.queues.size());
				//产品消费完成
				if (storage.isOK && storage.queues.isEmpty()) {
//                    System.out.println(name + ":storage.queues.isEmpty;" + conNum);
					break;
				}
			} catch (InterruptedException e) {
				System.err.println("push err:");
				e.printStackTrace();
			}
		}
	}
}

/**
 * 作者: 王坤造
 * 日期: 2017/1/3 17:25
 * 名称：消费者,消费的产品是Map对象且含有id
 * 备注：
 * bulkNum:匹量提交数量
 */
class ConsumerWithMapId implements Runnable {
	private Storage storage;
	private String name;
	private Integer bulkNum;
//    
//    

	public ConsumerWithMapId(Storage storage, Integer bulkNum, String name) {
		this.storage = storage;
		this.name = name;
		this.bulkNum = bulkNum;
	}

	public void run() {
		String index = null;
		String type = null;
		ArrayList<Object> maps = new ArrayList<>(bulkNum);
		ArrayList<String> ids = new ArrayList<>(bulkNum);
		//这里一定设置为true,不解释!!!
		while (true) {
			maps.clear();
			ids.clear();
			Product product = storage.pop();
			//第一次消费是时候,可能还没生产好,所以等待生产完成
			while (product == null) {
//                    System.out.println(name + ":product is null");
				//产品消费完成且生产者已经生产完成,则结束消费.
				if (storage.isOK) {
					System.out.println(name + ":storage.isOK");
					break;
				}
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}
			if (product != null) {
				index = product.index;
				type = product.type;
				maps.add(product.obj);
				ids.add(product.id);
				for (int i = 1; i < 7000; i++) {
					product = storage.pop();
					if (product == null) {
						System.out.println(name + ":product is null");
						if (storage.isOK) {
							System.out.println(name + ":storage.isOK");
							break;
						}
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						continue;
					}
					maps.add(product.obj);
					ids.add(product.id);
				}
				boolean b = ESUtils.operBulk2(index, type, maps, ids);
				if (!b) {
					System.err.println("bulk fail!");
				}
			}
			//System.out.println(name + " : "+storage.queues.size());
			//产品消费完成
			if (storage.isOK && storage.queues.isEmpty()) {
				System.out.println(name + ":storage.queues.isEmpty;");
				break;
			}
		}
	}
}

/**
 * 作者: 王坤造
 * 日期: 2017/1/3 17:25
 * 名称：消费者,消费的产品是Map对象且含有id
 * 备注：
 * bulkNum:匹量提交数量
 */
class ConsumerWithMap implements Runnable {
	private Storage storage;
	private String name;
	private Integer bulkNum;
	private String path;


	public ConsumerWithMap(Storage storage, Integer bulkNum, String name, String path) {
		this.storage = storage;
		this.name = name;
		this.bulkNum = bulkNum;
		this.path = path;
	}

	public void run() {
		String index = null;
		String type = null;
		ArrayList<Object> list = new ArrayList<>(bulkNum);
		//这里一定设置为true,不解释!!!
		while (true) {
			list.clear();
			Product product = storage.pop();
			Product currProduct = null;
			//第一次消费是时候,可能还没生产好,所以等待生产完成
			while (product == null) {
//                    System.out.println(name + ":product is null");
				//产品消费完成且生产者已经生产完成,则结束消费.
				if (storage.isOK) {
					System.out.println(name + ":storage.isOK");
					product = storage.pop();
					break;
				}
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				product = storage.pop();
				continue;
			}
			if (product != null) {
				index = product.index;
				type = product.type;
				currProduct = product;
				list.add(product.obj);
				for (int i = 1; i < bulkNum; i++) {
					product = storage.pop();
					if (product == null) {
						//System.out.println(name + ":product is null");
						if (storage.isOK) {
							System.out.println(name + ":storage.isOK");
							break;
						}
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						continue;
					}
					currProduct = product;
					list.add(product.obj);
				}
				boolean b = ESUtils.operBulk2(index, type, list);
//				if (b) {
//					if (currProduct != null) {
//						Object date = currProduct.field;
////						setSynchronizeData(path, date);
//					}
//				}
			}
			//System.out.println(name + " : "+storage.queues.size());
			//产品消费完成
			if (storage.isOK && storage.queues.isEmpty()) {
				System.out.println(name + ":storage.queues.isEmpty;");
				break;
			}
		}
	}

	private void setSynchronizeData(String path, List<Object> list) {
		try {
			BufferedWriter bufw = new BufferedWriter(new FileWriter(path));
			for (Object num : list) {
				bufw.append(num + ",");
			}
			bufw.flush();
			bufw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void setSynchronizeData(String path, Object num) {
		try {
			BufferedWriter bufw = new BufferedWriter(new FileWriter(path));
			bufw.append(num + ",");
			bufw.flush();
			bufw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

/**
 * 作者: 王坤造
 * 日期: 2017/1/3 17:24
 * 名称：生产者,传入的json串且含有id
 * 备注：
 */
class ProducerWithJsonId implements Runnable {
	private String index;
	private String type;
	private BlockingQueue<String> list;
	private Storage storage = null;
	private String idName = null;

	public ProducerWithJsonId(String index, String type, String idName, BlockingQueue<String> list, Storage storage) {
		this.index = index;
		this.type = type;
		this.idName = idName;
		this.list = list;
		this.storage = storage;
	}

	public void run() {
		while (!list.isEmpty()) {
			String json = list.poll();
			if (StringUtils.isEmpty(json)) {
//                System.out.println("get null value .");
				continue;
			}
			try {
				Map<String, Object> obj = ESUtils.JsonToObject(json, Map.class);
				storage.push(new Product(index, type, json, obj.get(idName).toString()));
				System.out.println("生产:\t" + storage.queues.size());
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("push err:");
			}
			//System.out.println(storage.queues.size());
		}
		storage.isOK = true;
	}
}

/**
 * 作者: 王坤造
 * 日期: 2017/1/3 17:24
 * 名称：生产者,传入的Map对象且含有id
 * 备注：
 */
class ProducerWithMap implements Runnable {
	private String index;
	private String type;
	private BlockingQueue<? extends Map> list;
	private Storage storage = null;
	private String[] excludeFields = null;
	private String dateField = null;

	public ProducerWithMap(String index, String type, String[] excludeFields, String dateField, BlockingQueue<? extends Map> maps, Storage storage) {
		this.index = index;
		this.type = type;
		this.excludeFields = excludeFields;
		this.list = maps;
		this.storage = storage;
		this.dateField = dateField;
	}

	public void run() {
		while (!list.isEmpty()) {
			Map<String, Object> map = list.poll();
			if (map == null || map.size() < 1) {
//                System.out.println("get null value .");
				continue;
			}
			if (!ArrayUtils.isEmpty(excludeFields)) {
				for (String field : excludeFields) {
					map.remove(field);
				}
			}
			storage.push(new Product(index, type, map, map.get(dateField)));
			//System.out.println("生产:\t" + storage.queues.size());
			//System.out.println(storage.queues.size());
		}
		storage.isOK = true;
		System.out.println("生产完成:\t" + storage.queues.size());
	}
}


/**
 * 作者: 王坤造
 * 日期: 2017/1/3 17:24
 * 名称：生产者,传入的Map对象且含有id
 * 备注：
 */
class ProducerWithMapId implements Runnable {
	private String index;
	private String type;
	private BlockingQueue<? extends Map> list;
	private Storage storage = null;
	private String idName = null;
	private String[] excludeFields = null;

	public ProducerWithMapId(String index, String type, String idName, String[] excludeFields, BlockingQueue<? extends Map> maps, Storage storage) {
		this.index = index;
		this.type = type;
		this.idName = idName;
		this.excludeFields = excludeFields;
		this.list = maps;
		this.storage = storage;
	}

	public void run() {
		while (!list.isEmpty()) {
			Map<String, Object> map = list.poll();
			if (map == null || map.size() < 1) {
//                System.out.println("get null value .");
				continue;
			}
			if (!ArrayUtils.isEmpty(excludeFields)) {
				for (String field : excludeFields) {
					map.remove(field);
				}
			}
			storage.push(new Product(index, type, map, map.get(idName).toString()));
			System.out.println("生产:\t" + storage.queues.size());
			//System.out.println(storage.queues.size());
		}
		storage.isOK = true;
	}
}


/**
 * 作者: 王坤造
 * 日期: 2017/1/3 16:52
 * 名称：仓库
 * 备注：存储相同index且type也相等的产品
 */
class Storage {
	public volatile boolean isOK = false;

	//不指定大小
	BlockingQueue<Product> queues = new LinkedBlockingQueue<Product>();

	/**
	 * 生产
	 *
	 * @param p 产品
	 * @throws InterruptedException
	 */
	public void push(Product p) {
		queues.offer(p);
	}

	/**
	 * 消费
	 *
	 * @return 产品
	 * @throws InterruptedException
	 */
	public Product pop() {
//        return queues.take();//获取不到会一直等待,线程就无法退出......
		return queues.poll();
	}
}

class Product {
	public String index;
	public String type;
	public String id;
	public String json;
	public Object obj;
	public Object field;

	public Product(String index, String type, String json, String id) {
		this.index = index;
		this.type = type;
		this.json = json;
		this.id = id;
	}

	public Product(String index, String type, Object obj, String id) {
		this.index = index;
		this.type = type;
		this.obj = obj;
		this.id = id;
	}

	public Product(String index, String type, Object obj, Object field) {
		this.index = index;
		this.type = type;
		this.obj = obj;
		this.field = field;
	}
}
