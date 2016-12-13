package SolrModel;

/**
 * 名称：王坤造
 * 时间：2016/12/9.
 * 名称：
 * 备注：
 */
public class Indication {
    private String id;
    private String code;
    private String standardNameCn;
    private String standardNameEn;
    private String alternativeName;
    private String diseaseProfile;

    private String icdTenCode;
    private Long level;
    private String therapyArea;
    private String specialty;
    private String commonDiseas;
    private String geneticDisease;
    private String indicationParentCodes;
    private String indicationSubCodes;
    private String parentIndicationNames;
    private String subIndicationNames;
    private String symptom;
    private Boolean isDelete;
    private String searchKey;
    private Long modifyTime;
    private String tags;
    private Long follerNum;
    private Boolean isFoller;

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

    public String getStandardNameCn() {
        return standardNameCn;
    }

    public void setStandardNameCn(String standardNameCn) {
        this.standardNameCn = standardNameCn;
    }

    public String getStandardNameEn() {
        return standardNameEn;
    }

    public void setStandardNameEn(String standardNameEn) {
        this.standardNameEn = standardNameEn;
    }

    public String getAlternativeName() {
        return alternativeName;
    }

    public void setAlternativeName(String alternativeName) {
        this.alternativeName = alternativeName;
    }

    public String getDiseaseProfile() {
        return diseaseProfile;
    }

    public void setDiseaseProfile(String diseaseProfile) {
        this.diseaseProfile = diseaseProfile;
    }

    public String getIcdTenCode() {
        return icdTenCode;
    }

    public void setIcdTenCode(String icdTenCode) {
        this.icdTenCode = icdTenCode;
    }

    public Long getLevel() {
        return level;
    }

    public void setLevel(Long level) {
        this.level = level;
    }

    public String getTherapyArea() {
        return therapyArea;
    }

    public void setTherapyArea(String therapyArea) {
        this.therapyArea = therapyArea;
    }

    public String getSpecialty() {
        return specialty;
    }

    public void setSpecialty(String specialty) {
        this.specialty = specialty;
    }

    public String getCommonDiseas() {
        return commonDiseas;
    }

    public void setCommonDiseas(String commonDiseas) {
        this.commonDiseas = commonDiseas;
    }

    public String getGeneticDisease() {
        return geneticDisease;
    }

    public void setGeneticDisease(String geneticDisease) {
        this.geneticDisease = geneticDisease;
    }

    public String getIndicationParentCodes() {
        return indicationParentCodes;
    }

    public void setIndicationParentCodes(String indicationParentCodes) {
        this.indicationParentCodes = indicationParentCodes;
    }

    public String getIndicationSubCodes() {
        return indicationSubCodes;
    }

    public void setIndicationSubCodes(String indicationSubCodes) {
        this.indicationSubCodes = indicationSubCodes;
    }

    public String getParentIndicationNames() {
        return parentIndicationNames;
    }

    public void setParentIndicationNames(String parentIndicationNames) {
        this.parentIndicationNames = parentIndicationNames;
    }

    public String getSubIndicationNames() {
        return subIndicationNames;
    }

    public void setSubIndicationNames(String subIndicationNames) {
        this.subIndicationNames = subIndicationNames;
    }

    public String getSymptom() {
        return symptom;
    }

    public void setSymptom(String symptom) {
        this.symptom = symptom;
    }

    public Boolean getDelete() {
        return isDelete;
    }

    public void setDelete(Boolean delete) {
        isDelete = delete;
    }

    public String getSearchKey() {
        return searchKey;
    }

    public void setSearchKey(String searchKey) {
        this.searchKey = searchKey;
    }

    public Long getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Long modifyTime) {
        this.modifyTime = modifyTime;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public Long getFollerNum() {
        return follerNum;
    }

    public void setFollerNum(Long follerNum) {
        this.follerNum = follerNum;
    }

    public Boolean getFoller() {
        return isFoller;
    }

    public void setFoller(Boolean foller) {
        isFoller = foller;
    }

}
