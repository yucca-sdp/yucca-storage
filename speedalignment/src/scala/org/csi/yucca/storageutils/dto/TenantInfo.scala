package org.csi.yucca.storageutils.dto

 @SerialVersionUID(120L)
class TenantInfo  extends Serializable {
  
  var tenantCode: String=null
  var organizationCode: String=null
	var solrCollection_DATA  : String=null
	var solrCollection_MEASURES: String=null
	var solrCollection_SOCIAL : String=null
	var solrCollection_MEDIA : String=null
  
  
  
  
}