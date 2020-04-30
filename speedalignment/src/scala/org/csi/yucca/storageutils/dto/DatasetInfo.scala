package org.csi.yucca.storageutils.dto
@SerialVersionUID(100L)
class DatasetInfo extends Serializable {
  
  var subType:String=null;
  var availableSpeed:Boolean=false
  var dbHiveSchema:String=null
  var dbHiveTable:String=null
  var datasetVersion:Integer=0
  var datasetCode:String=null
  var campi:List[Field]=null
  var idDataset:Integer=0
  
  var dataDomain:String=null
  var codSubDomain:String=null
  var tenantCode:String=null
  
  
  var streamCode:String=null
  var vESlug:String=null
  
  
}