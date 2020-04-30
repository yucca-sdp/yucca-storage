package org.csi.yucca.storageutils.dto
@SerialVersionUID(110L)
class Field (val name: String,val dtype:String) extends Serializable{
  
  
  var fieldName:String = name
  var dataType:String = dtype
  
  
}