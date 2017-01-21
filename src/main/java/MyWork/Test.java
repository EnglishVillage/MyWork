package MyWork;

import Utils.PropertiesUtils;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

//import java.time.Instant;
import java.io.*;
import java.util.*;

/**
 * Created by cube on 16-11-15.
 */
public class Test {
	public static void main(String[] argv) {
//        String index="twitter";
//        String type="tweet";
//        String id="1";
//        Object source = new MyWork.Person("aaa", 111, Date.from(Instant.now()),new ArrayList<MyWork.Book>(){{add(new MyWork.Book("Spark",99));}});
//        OldESUtils.operIndex(index,type,id,source);//设置id
//        OldESUtils.operIndex(index,type,null,source);//不设置id
//        OldESUtils.operIndex(index,type,null,null);//导入数据为空
//
//        OldESUtils.operGet(index,type,id);
//        OldESUtils.operGet(index,type,"2sdf");//id不存在
//        OldESUtils.operGet(index,type+"s",id);//type不存在
//        OldESUtils.operGet(index+"s",type,id);//index不存在
//
//        OldESUtils.operDelete(index,type,id);//id存在
//        OldESUtils.operDelete(index,type,"2sdf");//id不存在
//        OldESUtils.operDelete(index,type+"s",id);//type不存在
//        OldESUtils.operDelete(index+"s",type,id);//index不存在
		test3();
//		test2();
	}

	private static void test3() {
		String[] arr={"132","123432","62345"};
		Arrays.sort(arr);
		System.out.println(Arrays.toString(arr));
		ArrayList<Integer> list = new ArrayList<>();
		list.add(3);
		list.add(1);
		list.add(2);
		list.add(-1);
		Collections.sort(list);
		System.out.println(Arrays.toString(list.toArray(new Integer[]{})));
	}


	public static void test2() {
		Properties prop = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream("D:/MyDocument/Cube/Code/MyWork/src/main/resources/conf/solr.properties"));
			prop.load(in);
			FileOutputStream oFile = new FileOutputStream("D:/MyDocument/Cube/Code/MyWork/src/main/resources/conf/solr.properties");//true表示追加打开
			prop.setProperty("aaaaaaaa", "eeee");
			prop.store(oFile, "");
			oFile.close();
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}
