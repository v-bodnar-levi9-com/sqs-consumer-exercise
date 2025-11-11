from pydantic import BaseModel, Field
from typing import Union


class SQSMessageBody(BaseModel):    
    type: str = Field(..., description="Event type identifier")
    value: Union[int, float] = Field(..., description="Numeric value associated with the event")
    
    class Config:
        extra = "ignore"
