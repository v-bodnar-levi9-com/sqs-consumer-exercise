from pydantic import BaseModel, Field, ConfigDict
from typing import Union


class SQSMessageBody(BaseModel):
    model_config = ConfigDict(extra="ignore")

    type: str = Field(..., description="Event type identifier")
    value: Union[int, float] = Field(
        ..., description="Numeric value associated with the event"
    )


class StatsResponse(BaseModel):
    """Response model for stats endpoint"""

    event_type: str
    count: float
    total: float
    average: float = Field(description="Average value for this event type")


class EventStats(BaseModel):
    """Internal model for event statistics"""

    count: float = 0.0
    total: float = 0.0

    @property
    def average(self) -> float:
        return self.total / self.count if self.count > 0 else 0.0
