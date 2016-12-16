package MyWork;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * Created by cube on 16-11-15.
 */
public class LogModel {
    //主ID
    private long id;
    //次ID
    private int subId;
    private String systemName;
    private String host;

    //日志描述
    private String desc;
    private List<Integer> catIds;
    public LogModel(){
        Random random = new Random();
        this.setId(Math.abs(random.nextLong()));
        int subId = Math.abs(random.nextInt());
        this.setSubId(subId);
        List<Integer> list = new ArrayList<Integer>(5);
        for(int i=0;i<5;i++){
            list.add(Math.abs(random.nextInt()));
        }
        this.setCatIds(list);
        this.setSystemName(subId%1 == 0?"oa":"cms");
        this.setHost(subId%1 == 0?"10.0.0.1":"10.2.0.1");
        this.setDesc("中文" + UUID.randomUUID().toString());
    }
    public LogModel(long id,int subId,String sysName,String host,String desc,List<Integer> catIds){
        this.setId(id);
        this.setSubId(subId);
        this.setSystemName(sysName);
        this.setHost(host);
        this.setDesc(desc);
        this.setCatIds(catIds);
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getSubId() {
        return subId;
    }

    public void setSubId(int subId) {
        this.subId = subId;
    }

    /**
     * 系统名称
     */
    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    public List<Integer> getCatIds() {
        return catIds;
    }

    public void setCatIds(List<Integer> catIds) {
        this.catIds = catIds;
    }
}
