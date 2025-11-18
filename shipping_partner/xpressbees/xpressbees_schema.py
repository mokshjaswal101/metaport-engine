from pydantic import BaseModel, Json
from typing import Optional,List

# schema
from schema.base import DBBaseModel


class clientDetails_object(BaseModel):
    clientId: str
    clientName: str
    pickupVendorCode: int
    clientWarehouseId: str

class serviceDetails_object(BaseModel):
    serviceName: str
    serviceMode: str
    serviceVertical: str
    serviceType: str

class packageQuantity_object(BaseModel):
    value:int
    unit:str 
class collectableAmount_object(BaseModel):
    unit: str
    value: int

class helpContent_object(BaseModel):
    senderName: Optional[str] = None
    isOpenDelivery: Optional[str] = None
    isCommercialProperty: Optional[str] = None
    isDGShipmentType: Optional[str] = None

class pickUpInstruction_object(BaseModel):
    pickupType: str
    priorityRemarks: str
    isPickupPriority: str
    pickupInstruction: Optional[str] = None
    pickupSlotsDate: str

class packageDimension_object(BaseModel):
    length:packageQuantity_object
    width:packageQuantity_object
    height:packageQuantity_object

class packageWeight_object(BaseModel):
    physicalWeight:packageQuantity_object
    volumetricWeight:packageQuantity_object
    billableWeight:packageQuantity_object    

class packageDetails_object(BaseModel):
    packageDimension:packageDimension_object
    packageWeight:packageWeight_object

class customerDetails_object(BaseModel):
    countryType: str
    type: str
    country: str
    name: str
    addressLine: str
    pincode: int
    stateCountry: str
    city:Optional[str] = None

class contactDetails_object(BaseModel):
    type: str

class tinNumber_object(BaseModel):
    taxIdentificationNumber: str

class billFrom_object(BaseModel):
    customerDetails:customerDetails_object
    contactDetails:contactDetails_object
    tinNumber:tinNumber_object

class billTo_customerDetails_object(BaseModel):
    countryType: str
    type: str
    name: str
    addressLine: str
    country: str

class billTo_contactDetails_object(BaseModel):
    emailid: str
    type: str
    contactNumber: str
    virtualNumber:Optional[str] = None

class billTo_object(BaseModel):
    customerDetails:billTo_customerDetails_object
    contactDetails:billTo_contactDetails_object
    tinNumber:tinNumber_object    


      
class tax_list(BaseModel):
    taxType: str
    taxValue: int
    taxPercentage: int
class qcTemplateDetails_object(BaseModel):
    templateId: Optional[str] = None
    templateCategory: Optional[str] = None

class textCapture_list(BaseModel):
    label: Optional[str] = None
    type: Optional[str] = None
    valueToCheck: Optional[str] = None

class pickupProductImage_list(BaseModel):
    ImageUrl:str
    TextToShow:str

class captureImageRule_object(BaseModel):
    minImage: int
    maxImage: int  
class qcDetails_object(BaseModel):
    isQualityCheck: bool
    qcTemplateDetails:qcTemplateDetails_object
    textCapture:List[textCapture_list]
    pickupProductImage:List[pickupProductImage_list]
    captureImageRule:captureImageRule_object
    nonQcRVPType: str

class productDetails_List(BaseModel):
    productUniqueId: str
    productName: str
    productValue: str
    productDescription: str
    productCategory: str
    productQuantity: str
    tax:List[tax_list]
    hsnCode: str
    preTaxValue: int
    discount:int
    qcDetails:Optional[qcDetails_object]

class invoiceDetails_list(BaseModel):
    invoiceNumber: str  
    invoiceDate:str
    invoiceValue:int
    ebnExpDate: str
    ebnNumber: str
    billFrom:billFrom_object
    billTo:billTo_object
    productDetails:List[productDetails_List]
    
class orderDetails_list(BaseModel):
    orderNumber: str
    awbNumber:str
    subOrderNumber:str
    customerPromiseDate: str
    manifestId:str  
    collectableAmount: collectableAmount_object
    declaredAmount:packageQuantity_object
    helpContent:helpContent_object
    pickUpInstruction:pickUpInstruction_object
    packageDetails:packageDetails_object
    invoiceDetails:List[invoiceDetails_list]


 

class shipmentDetails_object(BaseModel):
    awbMpsGroupId: str
    packageType: str
    orderType: str
    partialRTOAllowed: bool
    allowPartialPickup: bool
    packageQuantity: packageQuantity_object
    totalWeight: packageQuantity_object
    orderDetails:List[orderDetails_list]
    

class address_list(BaseModel):
    country: str
    countryType: str
    name: str
    addressLine: str
    city: str
    stateCountry: str
    landmark: str
    pincode: str
    type: Optional[str] = None

class contactDetails_list(BaseModel):
    emailid: str
    type: str
    contactNumber: str
    virtualNumber: Optional[str] = None

class geoFencingInstruction_object(BaseModel):
    latitude: Optional[str] = None
    longitude: Optional[str] = None
    isGeoFencingEnabled: Optional[str] = None

class securityInstructions_object(BaseModel):
    securityCode: Optional[str] = None

class dropDetails_object(BaseModel):
    address:List[address_list]
    contactDetails:List[contactDetails_list]
    geoFencingInstruction:geoFencingInstruction_object
    securityInstructions:securityInstructions_object

class pickupDetails_object(BaseModel):
    address:List[address_list]
    contactDetails:List[contactDetails_list]
    geoFencingInstruction:geoFencingInstruction_object
    securityInstructions:securityInstructions_object 
    
class RTO_address_list(BaseModel):
    name: str
    addressLine:str
    landmark: str
    city: str
    stateCountry: str
    pincode: int
    countryType: str
    country: str
    type: str

class RTO_contactDetails_list(BaseModel):
    contactNumberExt: int
    contactNumber: int
    virtualNumber: int
    emailid: str
    type: str 

class RTO_customerTinDetails_object(BaseModel):
    taxIdentificationNumber: int
    taxIdentificationNumberType: str
    usage: int
    effictiveDate: Optional[str] = None
    expirationDate: Optional[str] = None  

class RTO_securityInstructions_object(BaseModel):
    isGenSecurityCode: bool
    securityCode: int

class RTODetails_geoFencingInstruction_object(BaseModel):
    isGeoFencingEnabled: Optional[str] = None
    latitude: int
    longitude: int

class RTODetails_object(BaseModel):
    address:List[RTO_address_list]
    contactDetails:List[RTO_contactDetails_list]
    customerTinDetails:RTO_customerTinDetails_object
    geoFencingInstruction:RTODetails_geoFencingInstruction_object
    securityInstructions:RTO_securityInstructions_object  

class shippingDetails_object(BaseModel):
    dropDetails:dropDetails_object
    pickupDetails:pickupDetails_object
    RTODetails:RTODetails_object



class Xpressbees_order_create_model(BaseModel):
    clientDetails:clientDetails_object
    serviceDetails:serviceDetails_object
    shipmentDetails:shipmentDetails_object
    bufferAttribute:List=[]
    shippingDetails:shippingDetails_object


class Xpressbees_order_cancel_model(BaseModel):
    ShippingID: str
    CancellationReason: str

class Xpressbees_track_order_model(BaseModel):
    AWBNumber:str  

  