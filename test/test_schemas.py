import pytest
from pydantic import ValidationError
from src.schemas import SQSMessageBody


class TestSQSMessageBodySchema:
    """Test cases for SQSMessageBody Pydantic schema"""
    
    def test_valid_message_body_with_int(self):
        """Test creating SQSMessageBody with valid integer value"""
        data = {
            "type": "user_signup",
            "value": 42
        }
        
        message = SQSMessageBody(**data)
        
        assert message.type == "user_signup"
        assert message.value == 42
        assert isinstance(message.value, int)
    
    def test_valid_message_body_with_float(self):
        """Test creating SQSMessageBody with valid float value"""
        data = {
            "type": "user_rating",
            "value": 4.5
        }
        
        message = SQSMessageBody(**data)
        
        assert message.type == "user_rating"
        assert message.value == 4.5
        assert isinstance(message.value, float)
    
    def test_valid_message_body_with_zero_value(self):
        """Test creating SQSMessageBody with zero value"""
        data = {
            "type": "reset_counter",
            "value": 0
        }
        
        message = SQSMessageBody(**data)
        
        assert message.type == "reset_counter"
        assert message.value == 0
    
    def test_valid_message_body_with_negative_value(self):
        """Test creating SQSMessageBody with negative value"""
        data = {
            "type": "adjustment",
            "value": -10.5
        }
        
        message = SQSMessageBody(**data)
        
        assert message.type == "adjustment"
        assert message.value == -10.5
    
    def test_missing_type_field(self):
        """Test validation error when type field is missing"""
        data = {
            "value": 42
        }
        
        with pytest.raises(ValidationError) as exc_info:
            SQSMessageBody(**data)
        
        error = exc_info.value
        assert len(error.errors()) == 1
        assert error.errors()[0]["type"] == "missing"
        assert "type" in error.errors()[0]["loc"]
    
    def test_missing_value_field(self):
        """Test validation error when value field is missing"""
        data = {
            "type": "user_signup"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            SQSMessageBody(**data)
        
        error = exc_info.value
        assert len(error.errors()) == 1
        assert error.errors()[0]["type"] == "missing"
        assert "value" in error.errors()[0]["loc"]
    
    def test_empty_type_field(self):
        """Test validation with empty type field"""
        data = {
            "type": "",
            "value": 42
        }
        
        # Empty string should be valid according to current schema
        message = SQSMessageBody(**data)
        assert message.type == ""
        assert message.value == 42
    
    def test_invalid_value_type(self):
        """Test validation error when value is not numeric"""
        data = {
            "type": "user_signup",
            "value": "not_a_number"
        }
        
        with pytest.raises(ValidationError) as exc_info:
            SQSMessageBody(**data)
        
        error = exc_info.value
        # Should have 2 errors: one for int parsing, one for float parsing
        assert len(error.errors()) == 2
        # Verify that both errors are about parsing the value field
        for err in error.errors():
            assert "value" in err["loc"]
    
    def test_none_values(self):
        """Test validation error with None values"""
        # Test None for type
        with pytest.raises(ValidationError):
            SQSMessageBody(type=None, value=42)
        
        # Test None for value
        with pytest.raises(ValidationError):
            SQSMessageBody(type="test", value=None)
    
    def test_extra_fields_ignored(self):
        """Test that extra fields are ignored due to Config.extra = 'ignore'"""
        data = {
            "type": "user_signup",
            "value": 42,
            "extra_field": "should_be_ignored",
            "another_extra": 123
        }
        
        message = SQSMessageBody(**data)
        
        assert message.type == "user_signup"
        assert message.value == 42
        # Extra fields should not be accessible
        assert not hasattr(message, "extra_field")
        assert not hasattr(message, "another_extra")
    
    def test_model_dump(self):
        """Test model serialization"""
        data = {
            "type": "user_signup",
            "value": 42
        }
        
        message = SQSMessageBody(**data)
        dumped = message.model_dump()
        
        assert dumped == data
        assert isinstance(dumped, dict)
    
    def test_model_dump_json(self):
        """Test JSON serialization"""
        data = {
            "type": "user_rating",
            "value": 4.5
        }
        
        message = SQSMessageBody(**data)
        json_str = message.model_dump_json()
        
        assert isinstance(json_str, str)
        # Should be valid JSON that can be parsed back
        import json
        parsed = json.loads(json_str)
        assert parsed == data
    
    def test_field_descriptions(self):
        """Test that field descriptions are properly set"""
        schema = SQSMessageBody.model_json_schema()
        
        assert "properties" in schema
        assert "type" in schema["properties"]
        assert "value" in schema["properties"]
        
        type_field = schema["properties"]["type"]
        value_field = schema["properties"]["value"]
        
        assert type_field["description"] == "Event type identifier"
        assert value_field["description"] == "Numeric value associated with the event"
    
    def test_string_coercion_to_number(self):
        """Test that string numbers are coerced to numeric types"""
        # Test string integer
        data = {
            "type": "test",
            "value": "42"
        }
        
        message = SQSMessageBody(**data)
        assert message.value == 42
        assert isinstance(message.value, int)
        
        # Test string float
        data = {
            "type": "test",
            "value": "4.5"
        }
        
        message = SQSMessageBody(**data)
        assert message.value == 4.5
        assert isinstance(message.value, float)
