package SolrModel;

/**
 * 名称：王坤造
 * 时间：2016/12/9.
 * 名称：
 * 备注：
 */
public class DiscoverLatestStage {
    private String id;
    private String code;
    private String latestStage;
    private Long unilateralCount;
    private Long cnCount;
    private Long combinationCount;
    private Long microInnovationCount;
    private Long biosimilarsCount;
    private Long totalCount;
    private Long countType;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Long getUnilateralCount() {
        return unilateralCount;
    }

    public void setUnilateralCount(Long unilateralCount) {
        this.unilateralCount = unilateralCount;
    }

    public Long getCnCount() {
        return cnCount;
    }

    public void setCnCount(Long cnCount) {
        this.cnCount = cnCount;
    }

    public Long getCombinationCount() {
        return combinationCount;
    }

    public void setCombinationCount(Long combinationCount) {
        this.combinationCount = combinationCount;
    }

    public Long getMicroInnovationCount() {
        return microInnovationCount;
    }

    public void setMicroInnovationCount(Long microInnovationCount) {
        this.microInnovationCount = microInnovationCount;
    }

    public Long getBiosimilarsCount() {
        return biosimilarsCount;
    }

    public void setBiosimilarsCount(Long biosimilarsCount) {
        this.biosimilarsCount = biosimilarsCount;
    }

    public String getLatestStage() {
        return latestStage;
    }

    public void setLatestStage(String latestStage) {
        this.latestStage = latestStage;
    }

    public Long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Long totalCount) {
        this.totalCount = totalCount;
    }

    public Long getCountType() {
        return countType;
    }

    public void setCountType(Long countType) {
        this.countType = countType;
    }

}
