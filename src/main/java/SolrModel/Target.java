package SolrModel;

/**
 * 名称：王坤造
 * 时间：2016/12/9.
 * 名称：
 * 备注：
 */
public class Target {
    private String id;
    private String code;
    private Long additionalAttribut;
    private String transferTo;
    private String abbreviation;
    private String standardName;
    private String nameCn;
    private String fullName;
    private String alternativeName;
    private String profile;
    private String parentTargetCode;
    private String parentTargetName;
    private String subTargetCodes;
    private String subTargetNames;
    private Boolean isDelete;
    private String transferToName;
    private Long modifyTime;
    private String searchKey;
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

    public Long getAdditionalAttribut() {
        return additionalAttribut;
    }

    public void setAdditionalAttribut(Long additionalAttribut) {
        this.additionalAttribut = additionalAttribut;
    }

    public String getTransferTo() {
        return transferTo;
    }

    public void setTransferTo(String transferTo) {
        this.transferTo = transferTo;
    }

    public String getAbbreviation() {
        return abbreviation;
    }

    public void setAbbreviation(String abbreviation) {
        this.abbreviation = abbreviation;
    }

    public String getStandardName() {
        return standardName;
    }

    public void setStandardName(String standardName) {
        this.standardName = standardName;
    }

    public String getNameCn() {
        return nameCn;
    }

    public void setNameCn(String nameCn) {
        this.nameCn = nameCn;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    public String getAlternativeName() {
        return alternativeName;
    }

    public void setAlternativeName(String alternativeName) {
        this.alternativeName = alternativeName;
    }

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public String getParentTargetCode() {
        return parentTargetCode;
    }

    public void setParentTargetCode(String parentTargetCode) {
        this.parentTargetCode = parentTargetCode;
    }

    public String getParentTargetName() {
        return parentTargetName;
    }

    public void setParentTargetName(String parentTargetName) {
        this.parentTargetName = parentTargetName;
    }

    public String getSubTargetCodes() {
        return subTargetCodes;
    }

    public void setSubTargetCodes(String subTargetCodes) {
        this.subTargetCodes = subTargetCodes;
    }

    public String getSubTargetNames() {
        return subTargetNames;
    }

    public void setSubTargetNames(String subTargetNames) {
        this.subTargetNames = subTargetNames;
    }

    public Boolean getDelete() {
        return isDelete;
    }

    public void setDelete(Boolean delete) {
        isDelete = delete;
    }

    public String getTransferToName() {
        return transferToName;
    }

    public void setTransferToName(String transferToName) {
        this.transferToName = transferToName;
    }

    public Long getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Long modifyTime) {
        this.modifyTime = modifyTime;
    }

    public String getSearchKey() {
        return searchKey;
    }

    public void setSearchKey(String searchKey) {
        this.searchKey = searchKey;
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
