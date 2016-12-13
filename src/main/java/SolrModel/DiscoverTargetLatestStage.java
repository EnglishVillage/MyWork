package SolrModel;

/**
 * 名称：王坤造
 * 时间：2016/12/9.
 * 名称：
 * 备注：
 */
public class DiscoverTargetLatestStage {
    private String id;
    private String code;
    private String latestStage;
    private Long chemicalCount;
    private Long biologyCount;
    private Long peptideCount;
    private Long otherCount;
    private Long totalCount;

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

    public String getLatestStage() {
        return latestStage;
    }

    public void setLatestStage(String latestStage) {
        this.latestStage = latestStage;
    }

    public Long getChemicalCount() {
        return chemicalCount;
    }

    public void setChemicalCount(Long chemicalCount) {
        this.chemicalCount = chemicalCount;
    }

    public Long getBiologyCount() {
        return biologyCount;
    }

    public void setBiologyCount(Long biologyCount) {
        this.biologyCount = biologyCount;
    }

    public Long getPeptideCount() {
        return peptideCount;
    }

    public void setPeptideCount(Long peptideCount) {
        this.peptideCount = peptideCount;
    }

    public Long getOtherCount() {
        return otherCount;
    }

    public void setOtherCount(Long otherCount) {
        this.otherCount = otherCount;
    }

    public Long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(Long totalCount) {
        this.totalCount = totalCount;
    }
}
