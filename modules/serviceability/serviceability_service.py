import http
from psycopg2 import DatabaseError
from sqlalchemy.orm import joinedload
import math
from fastapi.encoders import jsonable_encoder
import traceback
from decimal import Decimal


from context_manager.context import context_user_data, get_db_session

from logger import logger

# schema
from schema.base import GenericResponseModel
from modules.serviceability.serviceability_schema import (
    ServiceabilityParamsModel,
    CourierServiceabilityResponseModel,
    RateCalculatorParamsModel,
    RateCalculatorResponseModel,
)
from modules.client_contract.client_contract_schema import RateCardResponseModel

# models
from models import (
    Company_To_Client_Contract,
    Order,
    Return_Order,
    Pincode_Mapping,
    Company_To_Client_Rates,
    Company_To_Client_COD_Rates,
    New_Company_To_Client_Rate,
)

# data
from data.courier_service_mapping import courier_service_mapping


class ServiceabilityService:

    @staticmethod
    def get_available_couriers(
        serviceability_params: ServiceabilityParamsModel,
    ):

        try:
            # print("Welcome i am first section")
            rate_type = serviceability_params.shipment_type
            print(rate_type, "<<rate_type>>")

            user_data = context_user_data.get()
            client_id = user_data.client_id

            order_id = serviceability_params.order_id

            with get_db_session() as db:
                fetched_contracts = (
                    db.query(New_Company_To_Client_Rate)
                    .filter(
                        New_Company_To_Client_Rate.client_id == client_id,
                        New_Company_To_Client_Rate.isActive == True,
                    )
                    .options(joinedload(New_Company_To_Client_Rate.shipping_partner))
                    .all()
                )

                fetched_contracts = [
                    jsonable_encoder(contract) for contract in fetched_contracts
                ]
                available_contracts = []
                for contract in fetched_contracts:
                    freight = ServiceabilityService.calculate_freight(
                        order_id=order_id,
                        min_chargeable_weight=0.5,
                        additional_weight_bracket=0.5,
                        contract_id=contract["id"],
                        contract_data=contract,
                        rate_type=rate_type,
                    )
                    # Create the contract object with the calculated freight
                    contract_object = {
                        "name": contract["shipping_partner"]["name"],
                        "mode": contract["shipping_partner"]["mode"],
                        "logo": contract["shipping_partner"]["logo"],
                        "slug": contract["shipping_partner"]["slug"],
                        "freight": freight["freight"],
                        "courier_id": contract["id"],
                        "cod_charges": freight["cod_charges"],
                        "tax_amount": freight["tax_amount"],
                        "chargeable_weight": freight["chargeable_weight"],
                        "min_chargeable_weight": 0.5,
                        "additional_weight_bracket": 0.5,
                    }
                    # Add the contract object to the list
                    available_contracts.append(
                        CourierServiceabilityResponseModel(**contract_object)
                    )

                available_contracts.sort(
                    key=lambda contract: contract.freight
                    + contract.cod_charges
                    + contract.tax_amount
                )
                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=available_contracts,
                    message="Contracts fetched successfully",
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error getting available couriers: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetching the available couriers.",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def get_rate_card():

        try:

            user_data = context_user_data.get()
            client_id = user_data.client_id

            with get_db_session() as db:

                # fetched_contracts = (
                #     db.query(Company_To_Client_Contract)
                #     .filter(
                #         Company_To_Client_Contract.client_id == client_id,
                #         Company_To_Client_Contract.isActive == True,
                #     )
                #     .options(joinedload(Company_To_Client_Contract.aggregator_courier))
                #     .options(joinedload(Company_To_Client_Contract.rates))
                #     .options(joinedload(Company_To_Client_Contract.cod_rates))
                #     .all()
                # )

                fetched_contracts = (
                    db.query(New_Company_To_Client_Rate)
                    .join(New_Company_To_Client_Rate.aggregator_courier)
                    .filter(
                        New_Company_To_Client_Rate.client_id == client_id,
                        New_Company_To_Client_Rate.isActive == True,
                    )
                    .options(joinedload(New_Company_To_Client_Rate.aggregator_courier))
                    .all()
                )

                # print(jsonable_encoder(fetched_contracts))
                # print("**<<old>>**")
                # print(jsonable_encoder(fetched_contracts_new))
                # print("**<<new>>**")
                print("cross section 1")
                # fetched_contracts = [
                #     contract.to_model().model_dump()
                #     for contract in fetched_contracts_new
                # ]
                # print("cross section 2")
                # print(fetched_contracts, "<*<fetched_contracts>*>")

                rate_card = []

                zones = ["a", "b", "c", "d", "e"]
                contract_data = []

                for contract in jsonable_encoder(fetched_contracts):
                    ac = contract.get("aggregator_courier", {})
                    print(jsonable_encoder(ac))
                    courier_entry = {
                        "courier_name": ac.get("name", ""),
                        "aggregator_slug": ac.get("aggregator_slug", ""),
                        "mode": ac.get("mode", "").capitalize(),
                        "min_chargeable_weight": ac.get("min_chargeable_weight", 0.5),
                        "additional_weight_bracket": ac.get(
                            "additional_weight_bracket", 0.5
                        ),
                        "zones": [],
                        "cod_charges": {
                            "absolute": contract.get("absolute_rate", 0),
                            "percentage": contract.get("percentage_rate", 0),
                        },
                    }
                    for z in zones:
                        courier_entry["zones"].append(
                            {
                                "zone": z.upper(),
                                "forward": {
                                    "base": contract.get(f"base_rate_zone_{z}", 0),
                                    "additional": contract.get(
                                        f"additional_rate_zone_{z}", 0
                                    ),
                                },
                                "rto": {
                                    "base": contract.get(f"rto_base_rate_zone_{z}", 0),
                                    "additional": contract.get(
                                        f"rto_additional_rate_zone_{z}", 0
                                    ),
                                },
                            }
                        )
                    contract_data.append(courier_entry)

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=contract_data,
                    message="Rate Card fetched successfully",
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error fetching rate card: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while fetching the rate card.",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def get_pincode_details(
        pincode: int,
    ):

        try:

            with get_db_session() as db:

                # Optimized query: Select only needed columns and filter deleted records
                # The unique index on pincode ensures fast lookup
                pincode_data = (
                    db.query(
                        Pincode_Mapping.pincode,
                        Pincode_Mapping.city,
                        Pincode_Mapping.state,
                    )
                    .filter(
                        Pincode_Mapping.pincode == pincode,
                        Pincode_Mapping.is_deleted == False,
                    )
                    .first()
                )

                if not pincode_data:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.NOT_FOUND,
                        status=False,
                        message="Pincode not found",
                    )

                # create the response data for the pincode details
                # Note: city and state are stored in lowercase for consistency
                response_data = {
                    "pincode": pincode_data.pincode,
                    "city": pincode_data.city,
                    "state": pincode_data.state,
                    "country": "India",
                }

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=response_data,
                    message="Pincode data fetched successfully",
                )

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error fetching pincode details: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not fetch pincode details",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Error while fetching pincode details: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="Could not fetch pincode details",
            )

    @staticmethod
    def calculate_freight(
        order_id: str,
        min_chargeable_weight: float,
        additional_weight_bracket: float,
        contract_id: int,
        contract_data: dict,
        rate_type: str = "forward",
    ):

        try:
            client_id = context_user_data.get().client_id
            with get_db_session() as db:
                if rate_type == "reverse":
                    order_data = (
                        db.query(Return_Order)
                        .filter(
                            Return_Order.client_id == client_id,
                            Return_Order.order_id == order_id,
                        )
                        .first()
                    )
                else:
                    order_data = (
                        db.query(Order)
                        .filter(
                            Order.client_id == client_id, Order.order_id == order_id
                        )
                        .first()
                    )

                if not order_data:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="invalid data",
                    )

                applicable_weight = order_data.applicable_weight

                if applicable_weight < min_chargeable_weight:
                    applicable_weight = min_chargeable_weight

                zone = order_data.zone or "D"
                order_value = order_data.order_value
                payment_mode = order_data.payment_mode

                # Ensure the additional weight is only calculated if applicable_weight exceeds the minimum chargeable weight
                if float(applicable_weight) > float(min_chargeable_weight):
                    # weight that is above the min chargeable weight
                    additional_weight = float(applicable_weight) - float(
                        min_chargeable_weight
                    )

                    # Calculate how many additional weight brackets need to be charged
                    additional_bracket_count = math.ceil(
                        float(additional_weight) / float(additional_weight_bracket)
                    )

                    # Calculate the total chargeable weight
                    chargeable_weight = min_chargeable_weight + (
                        additional_weight_bracket * additional_bracket_count
                    )
                else:
                    # If applicable_weight is less than or equal to min_chargeable_weight, chargeable weight is the minimum
                    chargeable_weight = min_chargeable_weight
                    additional_bracket_count = 0
                zone = zone.lower()  # normalize to lowercase like "zone_a"
                base_rate_key = f"base_rate_zone_{zone}"
                additional_rate_key = f"additional_rate_zone_{zone}"

                base_rate = contract_data.get(base_rate_key, 0)
                additional_rate = contract_data.get(additional_rate_key, 0)
                # for min chargeable weight
                base_freight = float(base_rate)

                # for weight above the min chargeable weight
                additional_freight = float(additional_rate) * additional_bracket_count

                freight = base_freight + additional_freight

                # in case of COD orders, COD charge is also applicable
                # COD charge is calculated on the order value

                COD_freight = 0

                if payment_mode.lower() == "cod":
                    absolute_rate = Decimal(contract_data.get("absolute_rate", 0))
                    percentage_rate = Decimal(contract_data.get("percentage_rate", 0))
                    # cod charge is the maximum of the two values
                    COD_freight = float(
                        max(
                            absolute_rate,
                            (percentage_rate * Decimal(str(order_data.total_amount)))
                            * Decimal("0.01"),
                        )
                    )
                total_freight = freight + COD_freight
                tax = round(total_freight * 0.18, 2)

            return {
                "freight": round(freight, 2),
                "cod_charges": round(COD_freight, 2),
                "tax_amount": round(tax, 2),
                "chargeable_weight": chargeable_weight,
            }

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error calculating freight: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while calculating the freight.",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def get_contracts_calculator(ratecalculator_params: RateCalculatorParamsModel):
        try:
            print("**********************function started*****************************")

            rate_type = ratecalculator_params.shipment_type
            shipment_value = ratecalculator_params.shipment_value

            user_data = context_user_data.get()
            if not user_data:
                print("No context user data found!")
                raise Exception("No context user data found!")

            client_id = user_data.client_id
            print(f"Client ID is: {client_id}")
            print(f"ratecalculator_params: {ratecalculator_params}")
            length = ratecalculator_params.length
            breadth = ratecalculator_params.breadth
            height = ratecalculator_params.height
            actualWeight = ratecalculator_params.actualWeight
            paymentType = ratecalculator_params.paymentType

            print(
                f"Dimensions: length={length}, breadth={breadth}, height={height}, weight={actualWeight}"
            )
            print(f"Shipment Value: {shipment_value}, Rate Type: {rate_type}")

            with get_db_session() as db:
                fetched_contracts = (
                    db.query(Company_To_Client_Contract)
                    .filter(
                        Company_To_Client_Contract.client_id == client_id,
                        Company_To_Client_Contract.isActive == True,
                        Company_To_Client_Contract.rate_type == rate_type,
                    )
                    .options(joinedload(Company_To_Client_Contract.aggregator_courier))
                    .all()
                )

                if not fetched_contracts:
                    print("No active contracts found for client.")
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        status=False,
                        message="No active contracts found for this client.",
                    )

                print(f"Fetched {len(fetched_contracts)} contract(s)")
                array_fetched_couriers = []

                for contract in fetched_contracts:
                    courier = contract.aggregator_courier
                    if not courier:
                        print(
                            f"Skipping contract with missing aggregator_courier: {contract.id}"
                        )
                        continue

                    print(f"Calculating rate for courier: {courier.name}")

                    freight = ServiceabilityService.calculate_rate(
                        length=length,
                        breadth=breadth,
                        height=height,
                        actualWeight=actualWeight,
                        min_chargeable_weight=courier.min_chargeable_weight,
                        additional_weight_bracket=courier.additional_weight_bracket,
                        contract_id=contract.id,
                        shipment_type=rate_type,
                        paymentType=paymentType,
                        shipment_value=shipment_value,
                    )

                    if isinstance(freight, GenericResponseModel):
                        print(f"Rate calculation failed for courier {courier.name}")
                        continue

                    contract_object = {
                        "courier_name": courier.name,
                        "courier_id": contract.id,
                        "mode": courier.mode,
                        "logo": courier.logo,
                        "slug": courier.slug,
                        "base_freight": freight["freight"],
                        "cod_charges": freight["cod_charges"],
                        "tax_amount": freight["tax_amount"],
                        "total_freight": freight["total_freight"],
                        "chargeable_weight": freight["chargeable_weight"],
                        "min_chargeable_weight": courier.min_chargeable_weight,
                        "volumetric_weight": freight.get("volumetric_weight"),
                    }

                    array_fetched_couriers.append(
                        RateCalculatorResponseModel(**contract_object)
                    )

                array_fetched_couriers.sort(key=lambda c: c.total_freight)

                return GenericResponseModel(
                    status_code=http.HTTPStatus.OK,
                    status=True,
                    data=array_fetched_couriers,
                    message="Contracts fetched successfully for calculation",
                )

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg="DB Error in get_contracts_calculator: {}".format(str(e)),
            )
            raise e

        except Exception as e:
            import traceback

            print(f"Unhandled error in get_contracts_calculator:", e)
            print(traceback.format_exc())
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled exception: {}\n{}".format(
                    str(e), traceback.format_exc()
                ),
            )
            raise e

    @staticmethod
    def calculate_rate(
        length: float,
        breadth: float,
        height: float,
        actualWeight: float,
        min_chargeable_weight: float,
        additional_weight_bracket: float,
        contract_id: int,
        paymentType: str = "prepaid",
        shipment_value: float = 0,
        shipment_type: str = "forward",
    ):
        try:
            client_id = context_user_data.get().client_id
            print("Client ID fetched:", client_id)

            with get_db_session() as db:
                volumetric_weight = (length * breadth * height) / 5000
                chargeable_weight = max(
                    actualWeight, volumetric_weight, float(min_chargeable_weight)
                )  # convert here too
                print(
                    "Volumetric weight &&&&&&& chargeable_weight",
                    volumetric_weight,
                    chargeable_weight,
                )
                min_chargeable_weight = float(min_chargeable_weight)
                additional_weight_bracket = float(additional_weight_bracket)
                print(
                    "min_chargeable_weight &&&&&&& additional_weight_bracket",
                    min_chargeable_weight,
                    additional_weight_bracket,
                )
                additional_weight = max(0, chargeable_weight - min_chargeable_weight)
                additional_bracket_count = math.ceil(
                    additional_weight / additional_weight_bracket
                )
                print(
                    "additional_weight &&&&&&& additional_bracket_count",
                    additional_weight,
                    additional_bracket_count,
                )
                # Fetch freight rate
                freight_rate = (
                    db.query(Company_To_Client_Rates)
                    .filter(Company_To_Client_Rates.contract_id == contract_id)
                    .first()
                )

                if not freight_rate:
                    return GenericResponseModel(
                        status_code=http.HTTPStatus.BAD_REQUEST,
                        message="No freight rate found for the given contract.",
                    )

                base_freight = float(freight_rate.base_rate)
                print("i am base rate", base_freight)
                additional_freight = (
                    float(freight_rate.additional_rate) * additional_bracket_count
                )
                print("i am additional_freight rate", additional_freight)
                freight = base_freight + additional_freight
                print("i am total freight", freight)

                cod_charges = 0

                if paymentType.lower() == "cod":

                    cod_rate = (
                        db.query(Company_To_Client_COD_Rates)
                        .filter(
                            Company_To_Client_COD_Rates.contract_id == contract_id,
                        )
                        .first()
                    )

                    # cod charge is the maximum of the two values
                    if cod_rate:
                        print("COD Percentage Rate:", cod_rate.percentage_rate)
                        print("COD Absolute Rate:", cod_rate.absolute_rate)

                        cod_charges = float(
                            max(
                                float(cod_rate.absolute_rate),
                                float(cod_rate.percentage_rate)
                                * float(shipment_value)
                                * 0.01,
                            )
                        )

                total_freight = round(freight + cod_charges, 2)
                # print("i am total freight after tax", total_freight)
                print("i am COD charges", cod_charges)
                tax = round(total_freight * 0.18, 2)
                print("i am tax", tax)
                final_total = round(total_freight + tax, 2)
                print("i am final total", final_total)

                return {
                    "freight": round(freight, 2),
                    "cod_charges": round(cod_charges, 2),
                    "tax_amount": tax,
                    "total_freight": final_total,
                    "chargeable_weight": round(chargeable_weight, 2),
                    "volumetric_weight": round(volumetric_weight, 2),
                }

        except DatabaseError as e:
            logger.error(
                extra=context_user_data.get(),
                msg="Error calculating freight: {}".format(str(e)),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while calculating the freight.",
            )

        except Exception as e:
            import traceback

            print("Exception occurred:", e)
            print(traceback.format_exc())
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {} \n{}".format(str(e), traceback.format_exc()),
            )
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def check_price(
        client_id: int,
        order_id: str,
        contracts: list,
        weight: float,
    ):
        try:
            client_id = client_id
            with get_db_session() as db:
                order_data = (
                    db.query(Order)
                    .filter(Order.client_id == client_id, Order.order_id == order_id)
                    .first()
                )
                make_dict = []
                for contract in contracts:
                    min_chargeable_weight = (
                        contract.aggregator_courier.min_chargeable_weight
                    )
                    additional_weight_bracket = (
                        contract.aggregator_courier.additional_weight_bracket
                    )
                    applicable_weight = weight  # Coming Weight
                    if applicable_weight < min_chargeable_weight:
                        applicable_weight = min_chargeable_weight
                    zone = order_data.zone or "D"
                    payment_mode = order_data.payment_mode
                    if float(applicable_weight) > float(min_chargeable_weight):
                        additional_weight = float(applicable_weight) - float(
                            min_chargeable_weight
                        )
                        additional_bracket_count = math.ceil(
                            float(additional_weight) / float(additional_weight_bracket)
                        )
                    else:
                        additional_bracket_count = 0
                    freight_rate = (
                        db.query(Company_To_Client_Rates)
                        .filter(
                            Company_To_Client_Rates.contract_id == contract.id,
                            Company_To_Client_Rates.zone == zone,
                        )
                        .first()
                    )
                    base_freight = float(freight_rate.base_rate)
                    additional_freight = (
                        float(freight_rate.additional_rate) * additional_bracket_count
                    )
                    freight = base_freight + additional_freight
                    COD_freight = 0
                    if payment_mode.lower() == "cod":
                        COD_Rates = (
                            db.query(Company_To_Client_COD_Rates)
                            .filter(
                                Company_To_Client_COD_Rates.contract_id == contract.id,
                            )
                            .first()
                        )
                        COD_freight = float(
                            max(
                                COD_Rates.absolute_rate,
                                float(
                                    COD_Rates.percentage_rate
                                    * Decimal(str(order_data.total_amount))
                                )
                                * 0.01,
                            )
                        )
                    total_freight = freight + COD_freight
                    tax = round(total_freight * 0.18, 2)
                    make_dict.append(
                        {
                            "contract_id": contract.id,
                            "courier_name": contract.aggregator_courier.name,
                            "total_freight": total_freight,
                            "tax": round(tax * 0.18, 2),
                        }
                    )
                return sorted(make_dict, key=lambda x: x["total_freight"])

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error calculating freight: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while calculating the freight.",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

    @staticmethod
    def calculate_rto_freight(
        order_id: str,
        min_chargeable_weight: float,
        additional_weight_bracket: float,
        contract_id: int,
    ):
        try:

            client_id = context_user_data.get().client_id

            db = get_db_session()

            order_data = (
                db.query(Order)
                .filter(Order.client_id == client_id, Order.order_id == order_id)
                .first()
            )

            if not order_data:
                return GenericResponseModel(
                    status_code=http.HTTPStatus.BAD_REQUEST,
                    message="invalid data",
                )

            applicable_weight = order_data.applicable_weight

            if applicable_weight == 490:
                applicable_weight = 0.49

            if applicable_weight < min_chargeable_weight:
                applicable_weight = min_chargeable_weight

            zone = order_data.zone

            # weight that is above the min chargeable weight and will be charged based on the additional weight bracket
            additional_weight = float(applicable_weight) - float(min_chargeable_weight)
            additional_bracket_count = math.ceil(
                float(additional_weight) / float(additional_weight_bracket)
            )

            # fetchin the rates for this contract from the db
            freight_rate = (
                db.query(Company_To_Client_Rates)
                .filter(
                    Company_To_Client_Rates.contract_id == contract_id,
                    Company_To_Client_Rates.zone == zone,
                )
                .first()
            )

            # for min chargeable weight
            base_rto_freight = float(freight_rate.rto_base_rate)

            # for weight above the min chargeable weight
            additional_rto_freight = (
                float(freight_rate.rto_additional_rate) * additional_bracket_count
            )

            rto_freight = base_rto_freight + additional_rto_freight

            rto_tax = round(rto_freight * 0.18, 2)

            return {
                "rto_freight": round(rto_freight, 2),
                "rto_tax": round(rto_tax, 2),
            }

        except DatabaseError as e:
            # Log database error
            logger.error(
                extra=context_user_data.get(),
                msg="Error calculating freight: {}".format(str(e)),
            )

            # Return error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An error occurred while calculating the freight.",
            )

        except Exception as e:
            # Log other unhandled exceptions
            logger.error(
                extra=context_user_data.get(),
                msg="Unhandled error: {}".format(str(e)),
            )
            # Return a general internal server error response
            return GenericResponseModel(
                status_code=http.HTTPStatus.INTERNAL_SERVER_ERROR,
                message="An internal server error occurred. Please try again later.",
            )

        finally:
            if db:
                db.close()
