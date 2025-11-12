import pytest
import json
from pydantic import ValidationError

from src.shared.schemas import StatsResponse, EventStats, SQSMessageBody


class TestStatsResponse:
    """Test cases for the StatsResponse model"""

    def test_valid_stats_response(self):
        """Test creating a valid StatsResponse"""
        data = {
            "event_type": "user_signup",
            "count": 10.0,
            "total": 250.0,
            "average": 25.0,
        }

        response = StatsResponse(**data)

        assert response.event_type == "user_signup"
        assert response.count == 10.0
        assert response.total == 250.0
        assert response.average == 25.0

    def test_stats_response_field_types(self):
        """Test that fields are correctly typed"""
        response = StatsResponse(
            event_type="test",
            count=5,  # int should be converted to float
            total=100,  # int should be converted to float
            average=20.0,
        )

        assert isinstance(response.count, float)
        assert isinstance(response.total, float)
        assert isinstance(response.average, float)
        assert isinstance(response.event_type, str)

    def test_stats_response_zero_values(self):
        """Test StatsResponse with zero values"""
        response = StatsResponse(
            event_type="empty_event", count=0.0, total=0.0, average=0.0
        )

        assert response.count == 0.0
        assert response.total == 0.0
        assert response.average == 0.0

    def test_stats_response_negative_values(self):
        """Test StatsResponse with negative values"""
        response = StatsResponse(
            event_type="adjustment", count=2.0, total=-50.0, average=-25.0
        )

        assert response.total == -50.0
        assert response.average == -25.0

    def test_missing_required_fields(self):
        """Test validation errors when required fields are missing"""
        # Missing event_type
        with pytest.raises(ValidationError) as exc_info:
            StatsResponse(count=5.0, total=100.0, average=20.0)

        errors = exc_info.value.errors()
        assert any(
            error["type"] == "missing" and "event_type" in error["loc"]
            for error in errors
        )

        # Missing count
        with pytest.raises(ValidationError):
            StatsResponse(event_type="test", total=100.0, average=20.0)

        # Missing total
        with pytest.raises(ValidationError):
            StatsResponse(event_type="test", count=5.0, average=20.0)

        # Missing average
        with pytest.raises(ValidationError):
            StatsResponse(event_type="test", count=5.0, total=100.0)

    def test_invalid_field_types(self):
        """Test validation errors with invalid field types"""
        # Invalid count type
        with pytest.raises(ValidationError):
            StatsResponse(
                event_type="test", count="not_a_number", total=100.0, average=20.0
            )

        # Invalid total type
        with pytest.raises(ValidationError):
            StatsResponse(
                event_type="test", count=5.0, total="not_a_number", average=20.0
            )

    def test_string_numeric_conversion(self):
        """Test that string numbers are converted properly"""
        response = StatsResponse(
            event_type="test", count="5.0", total="100.5", average="20.1"
        )

        assert response.count == 5.0
        assert response.total == 100.5
        assert response.average == 20.1
        assert isinstance(response.count, float)

    def test_model_serialization(self):
        """Test model serialization to dict and JSON"""
        response = StatsResponse(
            event_type="user_login", count=15.0, total=300.0, average=20.0
        )

        # Test dict serialization
        dict_data = response.model_dump()
        expected_dict = {
            "event_type": "user_login",
            "count": 15.0,
            "total": 300.0,
            "average": 20.0,
        }
        assert dict_data == expected_dict

        # Test JSON serialization
        json_str = response.model_dump_json()
        parsed_data = json.loads(json_str)
        assert parsed_data == expected_dict

    def test_field_description(self):
        """Test that average field has proper description"""
        schema = StatsResponse.model_json_schema()

        assert "properties" in schema
        assert "average" in schema["properties"]

        average_field = schema["properties"]["average"]
        assert average_field["description"] == "Average value for this event type"


class TestEventStats:
    """Test cases for the EventStats model"""

    def test_valid_event_stats(self):
        """Test creating valid EventStats"""
        stats = EventStats(count=5.0, total=125.0)

        assert stats.count == 5.0
        assert stats.total == 125.0

    def test_event_stats_default_values(self):
        """Test EventStats with default values"""
        stats = EventStats()

        assert stats.count == 0.0
        assert stats.total == 0.0

    def test_event_stats_partial_initialization(self):
        """Test EventStats with partial initialization"""
        # Only count specified
        stats1 = EventStats(count=10.0)
        assert stats1.count == 10.0
        assert stats1.total == 0.0

        # Only total specified
        stats2 = EventStats(total=50.0)
        assert stats2.count == 0.0
        assert stats2.total == 50.0

    def test_average_property_calculation(self):
        """Test average property calculation"""
        # Normal case
        stats = EventStats(count=4.0, total=100.0)
        assert stats.average == 25.0

        # Zero count - should return 0
        stats_zero = EventStats(count=0.0, total=100.0)
        assert stats_zero.average == 0.0

        # Negative values
        stats_negative = EventStats(count=2.0, total=-10.0)
        assert stats_negative.average == -5.0

        # Fractional result
        stats_fraction = EventStats(count=3.0, total=10.0)
        assert abs(stats_fraction.average - 3.333333333333333) < 1e-10

    def test_average_property_edge_cases(self):
        """Test average property with edge cases"""
        # Both zero
        stats = EventStats(count=0.0, total=0.0)
        assert stats.average == 0.0

        # Very small numbers
        stats_small = EventStats(count=1e-10, total=1e-10)
        assert stats_small.average == 1.0

        # Very large numbers
        stats_large = EventStats(count=1e10, total=2e10)
        assert stats_large.average == 2.0

    def test_type_conversion(self):
        """Test that integer values are properly converted to floats"""
        stats = EventStats(count=5, total=100)

        assert isinstance(stats.count, float)
        assert isinstance(stats.total, float)
        assert stats.count == 5.0
        assert stats.total == 100.0

    def test_string_numeric_conversion(self):
        """Test conversion from string numbers"""
        stats = EventStats(count="7.5", total="150.25")

        assert stats.count == 7.5
        assert stats.total == 150.25
        assert isinstance(stats.count, float)
        assert isinstance(stats.total, float)

    def test_invalid_types(self):
        """Test validation errors with invalid types"""
        with pytest.raises(ValidationError):
            EventStats(count="not_a_number", total=100.0)

        with pytest.raises(ValidationError):
            EventStats(count=5.0, total="not_a_number")

    def test_model_serialization(self):
        """Test EventStats serialization"""
        stats = EventStats(count=3.0, total=75.0)

        # Test dict serialization
        dict_data = stats.model_dump()
        expected_dict = {"count": 3.0, "total": 75.0}
        assert dict_data == expected_dict

        # Test JSON serialization
        json_str = stats.model_dump_json()
        parsed_data = json.loads(json_str)
        assert parsed_data == expected_dict

    def test_computed_field_not_in_serialization(self):
        """Test that computed average field is not included in serialization"""
        stats = EventStats(count=4.0, total=100.0)

        dict_data = stats.model_dump()
        assert "average" not in dict_data

        # The average should only be accessible as a property
        assert stats.average == 25.0


class TestSQSMessageBodySharedSchema:
    """Test cases for SQSMessageBody in shared schemas (duplicate validation)"""

    def test_sqs_message_body_in_shared_module(self):
        """Test that SQSMessageBody works correctly in shared module"""
        message = SQSMessageBody(type="test_event", value=42.5)

        assert message.type == "test_event"
        assert message.value == 42.5

    def test_extra_fields_config_shared(self):
        """Test that extra fields are ignored in shared schema"""
        data = {"type": "test", "value": 42, "extra_field": "ignored"}

        message = SQSMessageBody(**data)
        assert message.type == "test"
        assert message.value == 42
        assert not hasattr(message, "extra_field")


class TestSchemaIntegration:
    """Test integration between different schema models"""

    def test_event_stats_to_stats_response_conversion(self):
        """Test converting EventStats to StatsResponse"""
        event_stats = EventStats(count=6.0, total=180.0)

        stats_response = StatsResponse(
            event_type="conversion_test",
            count=event_stats.count,
            total=event_stats.total,
            average=event_stats.average,
        )

        assert stats_response.event_type == "conversion_test"
        assert stats_response.count == 6.0
        assert stats_response.total == 180.0
        assert stats_response.average == 30.0

    def test_schema_validation_consistency(self):
        """Test that all schemas validate consistently"""
        # Test SQSMessageBody
        sqs_msg = SQSMessageBody(type="test", value=50.0)
        assert sqs_msg.model_dump()

        # Test EventStats
        event_stats = EventStats(count=2.0, total=100.0)
        assert event_stats.model_dump()

        # Test StatsResponse
        stats_response = StatsResponse(
            event_type="test", count=2.0, total=100.0, average=50.0
        )
        assert stats_response.model_dump()

    def test_json_serialization_roundtrip(self):
        """Test JSON serialization and deserialization roundtrip"""
        # Test EventStats roundtrip
        original_stats = EventStats(count=5.0, total=125.0)
        json_str = original_stats.model_dump_json()
        parsed_data = json.loads(json_str)
        restored_stats = EventStats(**parsed_data)

        assert restored_stats.count == original_stats.count
        assert restored_stats.total == original_stats.total
        assert restored_stats.average == original_stats.average

        # Test StatsResponse roundtrip
        original_response = StatsResponse(
            event_type="roundtrip_test", count=3.0, total=90.0, average=30.0
        )
        json_str = original_response.model_dump_json()
        parsed_data = json.loads(json_str)
        restored_response = StatsResponse(**parsed_data)

        assert restored_response.event_type == original_response.event_type
        assert restored_response.count == original_response.count
        assert restored_response.total == original_response.total
        assert restored_response.average == original_response.average
