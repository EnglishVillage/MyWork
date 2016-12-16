package MyWork;

import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;

/**
 * Created by cube on 16-11-15.
 */
public class Test {
    public static void main(String[] argv){
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


    }
}
