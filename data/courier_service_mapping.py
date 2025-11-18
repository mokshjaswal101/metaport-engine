# services
from shipping_partner.shiperfecto.shiperfecto import Shiperfecto
from shipping_partner.fship.fship import Fship
from shipping_partner.logistify.logistify import Logistify
from shipping_partner.delhivery.delhivery import Delhivery
from shipping_partner.shipmozo.shipmozo import Shipmozo
from shipping_partner.ats.ats import ATS
from shipping_partner.dtdc.dtdc import Dtdc
from shipping_partner.xpressbees.xpressbees import Xpressbees
from shipping_partner.ecom.ecom import Ecom
from shipping_partner.shiprocket.shiprocket import Shiprocket
from shipping_partner.ekart.ekart import Ekart
from shipping_partner.shadowfax.shadowfax import Shadowfax
from shipping_partner.bluedart.bluedart import Bluedart
from shipping_partner.zippyy.zippyy import Zippyy

courier_service_mapping = {
    "shiperfecto": Shiperfecto,
    "fship": Fship,
    "logistify": Logistify,
    "shipmozo": Shipmozo,
    "delhivery": Delhivery,
    "dtdc": Dtdc,
    "amazon": ATS,
    "xpressbees": Xpressbees,
    "ecom-express": Ecom,
    "shiprocket": Shiprocket,
    "shiprocket2": Shiprocket,
    "ats": ATS,
    "ekart": Ekart,
    "shadowfax": Shadowfax,
    "bluedart": Bluedart,
    "zippyy": Zippyy,
}
