package MyWork;

import Utils.PropertiesUtils;
import elasticsearch.api.ESUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import java.io.InputStream;
import java.util.*;

/**
 * 名称：王坤造
 * 时间：2017/1/5.
 * 名称：
 * 备注：
 */
public class SolrToES {
	public static void main(String[] args) {
		Timer timer = new Timer();
		TaskSolrToES task = new TaskSolrToES();
		long period;
		if (TaskSolrToES.getSyncdate() < 1) {
			period = 24;
		}
		period = TaskSolrToES.getSyncdate() * 3600 * 1000;
//		period = 10000;
		timer.schedule(task, 1000, period);
	}
}

class TaskSolrToES extends java.util.TimerTask {

	private static final Logger logger = Logger.getLogger(TaskSolrToES.class);
	private static final PropertiesUtils prop = new PropertiesUtils("conf/solr.properties",logger);

	private static String solrurl;//"http://192.168.1.136:8983/solr/";
	private static String[] indexes;//{"company", "discoverDrugs", "discoverDrugsCountry", "discoverDrugsNameDic", "discoverDrugsSalse", "discoverLatestStage", "discoverTargetLatestStage", "indication", "target"};
	//id字段
	private static String idName;//"id";
	//排除的字段,不插入到es中
	private static String[] excludefields;
	private static int bulknum;//10000;
	//同步时间
	private static int syncdate;

	public static int getSyncdate() {
		return syncdate;
	}

	/**
	 * 作者: 王坤造
	 * 日期: 2016/12/21 18:51
	 * 名称：从properties获取es集群配置
	 * 备注：
	 */
	static {
		solrurl = prop.getProperty("solrurl");
		indexes = StringUtils.split(prop.getProperty("indexes"), ',');
		idName = prop.getProperty("idname");
		excludefields = StringUtils.split(prop.getProperty("excludefields"), ',');
		bulknum = prop.getPropertyWithInteger("bulknum");
		syncdate = prop.getPropertyWithInteger("syncdate");
	}

	public static void getHttpSolrServer() throws Exception {
		for (String index : indexes) {
			HttpSolrServer solr = new HttpSolrServer(solrurl + index);
			solr.setMaxRetries(2);
			solr.setConnectionTimeout(5000);
			solr.setMaxTotalConnections(100);
			solr.setAllowCompression(true);

			SolrQuery query = new SolrQuery();
			query.setQuery("*:*");
			query.setRows(0);
			QueryResponse response = solr.query(query);
			SolrDocumentList results = response.getResults();
			//获取index总条数
			long totalCount = results.getNumFound();
			System.err.println("index:" + index + "\t总数:" + totalCount);
			logger.info("index:" + index + "\t总数:" + totalCount);

			String type = index.toLowerCase();
			String indexalias = type + "alias";

			List<String> jsons = new ArrayList<>();
			long totalTimeBegin = System.currentTimeMillis();

			if (StringUtils.isEmpty(idName)) {
				for (int i = 0; i < totalCount; i += bulknum) {
					jsons.clear();
					query.setStart(i);
					query.setRows(bulknum);
					response = solr.query(query);
					results = response.getResults();
					if (results.size() > 0) {
						for (int j = 0; j < results.size(); j++) {
							SolrDocument doc = results.get(j);
							addFields(doc);
							excludeFields(doc);
							jsons.add(ESUtils.ObjectToJson(doc));//得到单个对象json字符串
						}
						ESUtils.operBulk(indexalias, type, jsons);
						System.out.println("current insert:" + jsons.size());
						logger.info("current insert:" + jsons.size());
					} else {//空的集合就退出
						break;
					}
				}
			} else {
				List<String> ids = new ArrayList<>();
				for (int i = 0; i < totalCount; i += bulknum) {
					jsons.clear();
					ids.clear();
					query.setStart(i);
					query.setRows(bulknum);
					response = solr.query(query);
					results = response.getResults();
					if (results.size() > 0) {
						for (int j = 0; j < results.size(); j++) {
							SolrDocument doc = results.get(j);
							addFields(doc);
							excludeFields(doc);
							jsons.add(ESUtils.ObjectToJson(doc));//得到单个对象json字符串
							ids.add((String) doc.get(idName));//id
						}
						ESUtils.operBulk(indexalias, type, jsons, ids);
						System.out.println("current insert:" + jsons.size());
						logger.info("current insert:" + jsons.size());
					} else {//空的集合就退出
						break;
					}
				}
			}
			long totalTimeEnd = System.currentTimeMillis();
			System.out.println(index + "_Total:" + totalCount + ":\ttime:" + (totalTimeEnd - totalTimeBegin) + "ms");
			logger.info(index + "_Total:" + totalCount + ":\ttime:" + (totalTimeEnd - totalTimeBegin) + "ms"+System.lineSeparator());
		}
		System.out.println();
		logger.info(System.lineSeparator());
		System.out.println();
		logger.info(System.lineSeparator());
	}

	/**
	 * 作者: 王坤造
	 * 日期: 2017/1/10 11:22
	 * 名称：排除solr文档中不需要的字段
	 * 备注：
	 */
	private static void excludeFields(SolrDocument doc) {
		for (String excludefield : excludefields) {
			doc.remove(excludefield);
		}
	}


	/**
	 * 作者: 王坤造
	 * 日期: 2017/1/10 11:22
	 * 名称：排除solr文档中不需要的字段
	 * 备注：
	 */
	private static void addFields(SolrDocument doc) {
		if (!doc.containsKey("isDelete")) {
			doc.put("isDelete", false);
		}
	}

	@Override
	public void run() {
		try {
			getHttpSolrServer();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("error:",e);
		}
	}
}
