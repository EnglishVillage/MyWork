package SolrModel;

import java.io.Serializable;

/**
 * Created by deyumeng on 16/9/1.
 */
public class CompanyIndex implements Serializable {

    
    private String id;
    /**公司编码 **/
    
    private String code;
    /** 英文标准名称 **/
    
    private String standardNameEn;
    /** 中文标准名称**/
    
    private String standardNameCn;
    /** 公司英文全名**/
    
    private String fullNameEn;
    /** 公司中文全名**/
    
    private String fullNameCn;
    /** 公司特点简介**/
    
    private String profile;
    /** 股票交易所信息**/
    
    private String stock;
    /** 成立时间**/
    
    private String yearFounded;
    /** 总部地址**/
    
    private String headquarters;
    /** 资本类型**/
    
    private String capitalType;
    /** 公司层级 (Company Level)**/
    
    private Integer companyLevel;
    /** 异名**/
    
    private String alternativeName;
    /** 美国申报名称**/
    
    private String nameFDA;
    /** 并购说明**/
    
    private String maDetails;
    /**上级公司 **/
    
    private String parentCompanys;
    /**下属公司 **/
    
    private String childrenCompanys;
    /** 公司官网**/
    
    private String urlAddr;
    
    private String tags;
    
    private Long addTime;
    
    private Long modifyTime;
    
    private String des;
    
    private String headPic;

    /** 数据有效性**/
    
    private Boolean isDelete;
    
    private String parentCodes;
    
    private String subCodes;
    
    private String transferCode;
    
    private String transferName;
    /** 相关基金编码**/
    
    private String fundCodes;
    /** 相关基金名称**/
    
    private String fundNames;

    private Integer follerNum;

    private Boolean isFoller;

    
    private String searchKey;


    public String getSearchKey() {
        return searchKey;
    }

    public void setSearchKey(String searchKey) {
        this.searchKey = searchKey;
    }

    public Integer getFollerNum() {
        return follerNum;
    }

    public void setFollerNum(Integer follerNum) {
        this.follerNum = follerNum;
    }

    public Boolean getIsFoller() {
        return isFoller;
    }

    public void setIsFoller(Boolean isFoller) {
        this.isFoller = isFoller;
    }

    public String getParentCodes() {
        return parentCodes;
    }

    public void setParentCodes(String parentCodes) {
        this.parentCodes = parentCodes;
    }

    public String getSubCodes() {
        return subCodes;
    }

    public void setSubCodes(String subCodes) {
        this.subCodes = subCodes;
    }

    public String getFundCodes() {
        return fundCodes;
    }

    public void setFundCodes(String fundCodes) {
        this.fundCodes = fundCodes;
    }

    public String getFundNames() {
        return fundNames;
    }

    public void setFundNames(String fundNames) {
        this.fundNames = fundNames;
    }

    public String getTransferName() {
        return transferName;
    }

    public void setTransferName(String transferName) {
        this.transferName = transferName;
    }

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

    public String getStandardNameEn() {
        return standardNameEn;
    }

    public void setStandardNameEn(String standardNameEn) {
        this.standardNameEn = standardNameEn;
    }

    public String getStandardNameCn() {
        return standardNameCn;
    }

    public void setStandardNameCn(String standardNameCn) {
        this.standardNameCn = standardNameCn;
    }

    public String getFullNameEn() {
        return fullNameEn;
    }

    public void setFullNameEn(String fullNameEn) {
        this.fullNameEn = fullNameEn;
    }

    public String getFullNameCn() {
        return fullNameCn;
    }

    public void setFullNameCn(String fullNameCn) {
        this.fullNameCn = fullNameCn;
    }

    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public String getStock() {
        return stock;
    }

    public void setStock(String stock) {
        this.stock = stock;
    }

    public String getYearFounded() {
        return yearFounded;
    }

    public void setYearFounded(String yearFounded) {
        this.yearFounded = yearFounded;
    }

    public String getHeadquarters() {
        return headquarters;
    }

    public void setHeadquarters(String headquarters) {
        this.headquarters = headquarters;
    }

    public String getCapitalType() {
        return capitalType;
    }

    public void setCapitalType(String capitalType) {
        this.capitalType = capitalType;
    }

    public Integer getCompanyLevel() {
        return companyLevel;
    }

    public void setCompanyLevel(Integer companyLevel) {
        this.companyLevel = companyLevel;
    }

    public String getAlternativeName() {
        return alternativeName;
    }

    public void setAlternativeName(String alternativeName) {
        this.alternativeName = alternativeName;
    }

    public String getNameFDA() {
        return nameFDA;
    }

    public void setNameFDA(String nameFDA) {
        this.nameFDA = nameFDA;
    }

    public String getMaDetails() {
        return maDetails;
    }

    public void setMaDetails(String maDetails) {
        this.maDetails = maDetails;
    }

    public String getParentCompanys() {
        return parentCompanys;
    }

    public void setParentCompanys(String parentCompanys) {
        this.parentCompanys = parentCompanys;
    }

    public String getChildrenCompanys() {
        return childrenCompanys;
    }

    public void setChildrenCompanys(String childrenCompanys) {
        this.childrenCompanys = childrenCompanys;
    }

    public String getUrlAddr() {
        return urlAddr;
    }

    public void setUrlAddr(String urlAddr) {
        this.urlAddr = urlAddr;
    }

    public String getDes() {
        return des;
    }

    public void setDes(String des) {
        this.des = des;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public Long getAddTime() {
        return addTime;
    }

    public void setAddTime(Long addTime) {
        this.addTime = addTime;
    }

    public Long getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Long modifyTime) {
        this.modifyTime = modifyTime;
    }


    public String getHeadPic() {
        return headPic;
    }

    public void setHeadPic(String headPic) {
        this.headPic = headPic;
    }

    public Boolean getIsDelete() {
        return isDelete;
    }

    public void setIsDelete(Boolean isDelete) {
        this.isDelete = isDelete;
    }

    public String getTransferCode() {
        return transferCode;
    }

    public void setTransferCode(String transferCode) {
        this.transferCode = transferCode;
    }

}
