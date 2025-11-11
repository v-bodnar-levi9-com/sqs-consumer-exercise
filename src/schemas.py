from pydantic import BaseModel, Field, ConfigDict
from typing import Union


class SQSMessageBody(BaseModel):    
    model_config = ConfigDict(extra="ignore")
    
    type: str = Field(..., description="Event type identifier")
    value: Union[int, float] = Field(..., description="Numeric value associated with the event")
