package MyWork;

import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.client.http.JestHttpClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin;
import solutions.siren.join.SirenJoinPlugin;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 作者: 王坤造
 * 日期: 2016/12/5 11:21
 * 名称：获取es客户端
 * 备注：至少需要2个参数:集群名称,es集群中一台节点的ip端口号
 */
public class ESClientHelperJest {

    //集群名称(testes)
    private static String clusterName = null;
    //存储es的ip,host(192.168.2.150:9300)
    private static HashMap<String, Integer> ips = null;

    /**
     * 作者: 王坤造
     * 日期: 2016/12/21 18:51
     * 名称：从properties获取es集群配置
     * 备注：
     */
    static {
        final Properties prop = new Properties();
        try (InputStream inputStream = ClassLoader.getSystemResourceAsStream("conf/es.properties");) {
            prop.load(inputStream);
            clusterName = prop.getProperty("clusterName");
            ips = new HashMap<String, Integer>() {{
                put(prop.getProperty("host"), Integer.parseInt(prop.getProperty("urlport")));
            }};
        } catch (Exception e) {
            e.printStackTrace();
        }
    }




    private ConcurrentHashMap<String, JestHttpClient> clientMap = new ConcurrentHashMap<>();

    private ESClientHelperJest() {
        if (clientMap.size() < 1) {
            init();
        }
    }

    private void init() {
        String url=null;
        for (String s : ips.keySet()) {
            url="http://"+s+":"+ips.get(s);
        }
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(url)
                .discoveryEnabled(true)//开启发现
                //.discoveryFrequency(500l, TimeUnit.MILLISECONDS)//发现频率
                //.clusterName("")//这个不用设置???
                .multiThreaded(true)
                //.maxTotalConnection(5)
                .build());
        JestHttpClient client = (JestHttpClient) factory.getObject();
        clientMap.put(clusterName,client);
    }

    public JestHttpClient getClient() {
        return clientMap.get(clusterName);
    }

    public static final ESClientHelperJest getInstance() {
        return ClientHolder.INSTANCE;
    }

    public static void closeClient(JestHttpClient client) {
        client.shutdownClient();
    }


    private static class ClientHolder {
        private static final ESClientHelperJest INSTANCE = new ESClientHelperJest();
    }
}