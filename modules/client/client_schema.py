from pydantic import BaseModel, Field, EmailStr, root_validator
from typing import Optional, Any
from datetime import datetime


# schema
from schema.base import DBBaseModel


class ClientModel(DBBaseModel):
    client_name: str
    client_code: str
    company_id: int


class ClientInsertModel(BaseModel):
    clientName: str
    userFullName: str
    userEmail: str
    userPhone: str
    password: str
    walletType: str
    creditLimit: float


class ClientResponseModel(BaseModel):
    client_name: str
    order_status_counts: Any


class getClientFiltersModel(BaseModel):
    start_date: datetime
    end_date: datetime


# FOR  ONBOARDING PROCESS


class OtpVerified(BaseModel):
    otp: str = Field(..., min_length=6, max_length=6, pattern=r"^\d+$")


class OnBoardingForm(BaseModel):
    city: Optional[str] | None = None
    country: Optional[str] | None = None
    email: Optional[EmailStr] | None = None
    company_legal_name: Optional[str] | None = None
    company_name: Optional[str] | None = None
    landmark: Optional[str] | None = None
    phone_number: Optional[int] | None = None
    pincode: Optional[int] | None = None
    office_address: Optional[str] | None = None
    state: Optional[str] | None = None
    beneficiary_name: Optional[str] = None
    bank_name: Optional[str] = None
    account_no: Optional[str] = None
    account_type: Optional[str] = None
    ifsc_code: Optional[str] = None
    # pan_number: Optional[str] = None
    gst: Optional[str] = None
    upload_gst: Optional[str] = None
    is_cod_order: Optional[bool] = None
    upload_pan: Optional[str] = None
    upload_cheque: Optional[str] = None
    # COI
    is_coi: Optional[bool] = None
    coi: Optional[str] = None
    upload_coi: Optional[str] = None

    is_gst: Optional[bool] = None
    aadhar_card: Optional[int] = None
    upload_aadhar_card_front: Optional[str] = None
    upload_aadhar_card_back: Optional[str] = None
    stepper: int

    @root_validator(pre=True)
    def validate_stepper(cls, values):
        step = values.get("stepper")
        required_fields = {
            2: [
                "city",
                # "country",
                "email",
                "company_legal_name",
                "company_name",
                "phone_number",
                "pincode",
                "office_address",
                "state",
            ],
        }
        for field in required_fields.get(step, []):
            if not values.get(field):
                raise ValueError(f"{field} is required for step {step}")
        return values


class SignupwithOnboarding(BaseModel):
    company_name: str
    phone_number: int
    email: EmailStr
    client_id: int


class OnboardingGetByStatus(BaseModel):
    status: str


class OnboardingActionSchema(BaseModel):
    action: str
    remarks: Optional[str] = None


class OnBoardingPreviousSchema(BaseModel):
    stepper: int
    action: str


class OnBoardingFormStepTwoUpdateSchema(BaseModel):
    company_legal_name: str
    company_name: str
    landmark: str
    pincode: int
    city: str
    state: str
    country: str
    phone_number: int
    email: str


# Response schema for complete client details
class ClientOnboardingDetailsModel(BaseModel):
    id: Optional[int] = None
    onboarding_user_id: Optional[int] = None
    client_id: Optional[int] = None
    company_legal_name: Optional[str] = None
    company_name: Optional[str] = None
    office_address: Optional[str] = None
    landmark: Optional[str] = None
    pincode: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    phone_number: Optional[str] = None
    email: Optional[str] = None
    pan_number: Optional[str] = None
    # upload_pan: Optional[str] = None
    is_coi: Optional[bool] = None
    coi: Optional[str] = None
    upload_coi: Optional[str] = None
    is_gst: Optional[bool] = None
    gst: Optional[str] = None
    upload_gst: Optional[str] = None
    aadhar_card: Optional[str] = None
    upload_aadhar_card_front: Optional[str] = None
    upload_aadhar_card_back: Optional[str] = None
    is_cod_order: Optional[bool] = None
    is_stepper: Optional[int] = None
    is_company_details: Optional[bool] = None
    is_billing_details: Optional[bool] = None
    is_term: Optional[bool] = None
    is_review: Optional[bool] = None
    is_form_access: Optional[bool] = None


class ClientBankDetailsModel(BaseModel):
    id: Optional[int] = None
    user_id: Optional[int] = None
    client_id: Optional[int] = None
    client_onboarding_id: Optional[int] = None
    beneficiary_name: Optional[str] = None
    bank_name: Optional[str] = None
    account_no: Optional[str] = None
    account_type: Optional[str] = None
    ifsc_code: Optional[str] = None
    upload_cheque: Optional[str] = None


class CompleteClientDetailsModel(BaseModel):
    client_data: ClientModel
    onboarding_details: Optional[ClientOnboardingDetailsModel] = None
    # bank_details: Optional[ClientBankDetailsModel] = None
