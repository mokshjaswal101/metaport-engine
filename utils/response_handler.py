from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from schema.base import GenericResponseModel

from context_manager.context import context_user_data

from logger import logger


# build a proper api response from the Generic response sent to it
def build_api_response(generic_response: GenericResponseModel) -> JSONResponse:
    try:
        response_json = jsonable_encoder(generic_response)

        # Remove the status_code key if it exists
        response_json.pop("status_code", None)

        res = JSONResponse(
            status_code=generic_response.status_code, content=response_json
        )

        logger.info(
            extra=context_user_data.get(),
            msg="build_api_response: Generated Response with status_code:"
            + f"{generic_response.status_code}",
        )
        return res

    except Exception as e:
        logger.error(
            extra=context_user_data.get(),
            msg=f"Exception in build_api_response error : {e}",
        )

        return JSONResponse(status_code=generic_response.status_code, content=str(e))
