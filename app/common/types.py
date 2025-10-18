from pydantic import BaseModel
from typing import Optional

class ErrorDetail(BaseModel):
    code: str
    message: str
    error_id: Optional[str] = None

class ApiResponse(BaseModel):
    success: bool
    data: Optional[dict] = None
    error: Optional[ErrorDetail] = None