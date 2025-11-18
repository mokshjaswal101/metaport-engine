from sqlalchemy import Column, String, Integer, Boolean
from sqlalchemy.orm import Session, relationship

from database import DBBaseClass, DBBase

import uuid as uuid


class Pincode_Serviceability(DBBase, DBBaseClass):

    __tablename__ = "pincode_serviceability"

    pincode = Column(Integer, nullable=False, default=False)
    dtdc_surface_fm = Column(Boolean, nullable=False, default=False)
    dtdc_surface_lm_prepaid = Column(Boolean, nullable=False, default=False)
    dtdc_surface_lm_cod = Column(Boolean, nullable=False, default=False)
    dtdc_air_fm = Column(Boolean, nullable=False, default=False)
    dtdc_air_lm_prepaid = Column(Boolean, nullable=False, default=False)
    dtdc_air_lm_cod = Column(Boolean, nullable=False, default=False)
    ekart_fm = Column(Boolean, nullable=False, default=False)
    ekart_lm_prepaid = Column(Boolean, nullable=False, default=False)
    ekart_lm_cod = Column(Boolean, nullable=False, default=False)
    ats_fm = Column(Boolean, nullable=False, default=False)
    ats_lm = Column(Boolean, nullable=False, default=False)
    shadowfax_fm = Column(Boolean, nullable=False, default=False)
    shadowfax_lm = Column(Boolean, nullable=False, default=False)
    delhivery_fm = Column(Boolean, nullable=False, default=False)
    delhivery_lm_prepaid = Column(Boolean, nullable=False, default=False)
    delhivery_lm_cod = Column(Boolean, nullable=False, default=False)
