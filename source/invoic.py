#!/usr/bin/env python3
"""
EDIFACT INVOIC Generator
A comprehensive implementation for generating EDIFACT INVOIC messages.
"""

import datetime
import logging
import json
import argparse
import re
from decimal import Decimal, InvalidOperation
from typing import List, Dict, Any, Optional, Tuple
import io
import os
import codecs
from jsonschema import validate, ValidationError

class EDIFACTConfig:
    """Configuration for EDIFACT generation with enhanced flexibility"""
    
    DEFAULT_DATA_SEPARATOR = "+"
    DEFAULT_COMPONENT_SEPARATOR = ":"
    DEFAULT_SEGMENT_TERMINATOR = "'"
    DEFAULT_DECIMAL_NOTATION = "."
    DEFAULT_PRECISION = 2
    INVOIC_DOCUMENT_TYPE = "380"
    ORIGINAL_DOCUMENT = "9"
    VAT_TAX_CATEGORY = "VAT"
    SALES_TAX_CATEGORY = "SAL"
    BANK_TRANSFER_PAYMENT = "5"
    CREDIT_CARD_PAYMENT = "1"
    CASH_PAYMENT = "10"
    DEFAULT_EDI_VERSION = "D:96A:UN"
    DEFAULT_CHARACTER_SET = "UNOA"
    DEFAULT_DATE_FORMAT = "%Y%m%d"
    DEFAULT_FILE_ENCODING = "utf-8"
    DEFAULT_MESSAGE_REF_PREFIX = "INV"
    MAX_PRODUCT_CODE_LENGTH = 35
    MAX_DESCRIPTION_LENGTH = 70
    MAX_NAME_LENGTH = 35
    MAX_ADDRESS_LINE_LENGTH = 35
    MAX_CITY_LENGTH = 35
    MAX_COUNTRY_LENGTH = 3
    MAX_QUANTITY = 999999
    MAX_PRICE = 9999999.99
    VALID_PARTY_QUALIFIERS = {"BY", "SU", "IV", "DP", "PE"}
    VALID_PAYMENT_METHODS = {BANK_TRANSFER_PAYMENT, CREDIT_CARD_PAYMENT, CASH_PAYMENT}
    VALID_CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF", "CNY"}
    VALID_COUNTRIES = {"US", "GB", "FR", "DE", "IT", "ES", "NL", "BE", "CN", "JP", "AU", "CA"}
    VALID_ENCODINGS = {"utf-8", "ascii", "iso-8859-1"}
    UNOA_ALLOWED_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 -.&'()+,:;=?@")
    SEGMENT_UNA = "UNA"
    SEGMENT_UNB = "UNB"
    SEGMENT_UNH = "UNH"
    SEGMENT_BGM = "BGM"
    SEGMENT_DTM = "DTM"
    SEGMENT_NAD = "NAD"
    SEGMENT_CUX = "CUX"
    SEGMENT_RFF = "RFF"
    SEGMENT_LIN = "LIN"
    SEGMENT_IMD = "IMD"
    SEGMENT_QTY = "QTY"
    SEGMENT_PRI = "PRI"
    SEGMENT_TAX = "TAX"
    SEGMENT_MOA = "MOA"
    SEGMENT_PAT = "PAT"
    SEGMENT_ALC = "ALC"
    SEGMENT_UNT = "UNT"
    SEGMENT_UNZ = "UNZ"

class EDIFACTGeneratorError(Exception):
    """Custom exception for EDIFACT generation errors"""
    pass

class EDIFACTValidator:
    """Handles validation of EDIFACT data with enhanced validation"""
    
    JSON_SCHEMA = {
        "type": "object",
        "required": ["message_ref", "invoice_number", "invoice_date", "parties", "items"],
        "properties": {
            "message_ref": {"type": "string", "minLength": 1},
            "invoice_number": {"type": "string", "minLength": 1},
            "invoice_date": {"type": "string", "pattern": r"^\d{8}$"},
            "currency": {"type": "string", "enum": list(EDIFACTConfig.VALID_CURRENCIES)},
            "reference": {"type": "string", "minLength": 1},
            "parties": {
                "type": "array",
                "minItems": 2,
                "items": {
                    "type": "object",
                    "required": ["qualifier", "id"],
                    "properties": {
                        "qualifier": {"type": "string", "enum": list(EDIFACTConfig.VALID_PARTY_QUALIFIERS)},
                        "id": {"type": "string", "minLength": 1},
                        "name": {"type": "string"},
                        "street": {"type": "string"},
                        "city": {"type": "string"},
                        "country": {"type": "string", "enum": list(EDIFACTConfig.VALID_COUNTRIES)}
                    }
                }
            },
            "items": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "required": ["product_code", "description", "quantity", "price"],
                    "properties": {
                        "product_code": {"type": "string", "minLength": 1},
                        "description": {"type": "string", "minLength": 1},
                        "quantity": {"type": ["string", "number"], "minimum": 0},
                        "price": {"type": ["string", "number"], "minimum": 0},
                        "tax_rate": {"type": ["string", "number"], "minimum": 0},
                        "unit": {"type": "string"}
                    }
                }
            },
            "payment_terms": {
                "type": "object",
                "properties": {
                    "due_date": {"type": "string", "pattern": r"^\d{8}$"},
                    "method": {"type": "string", "enum": list(EDIFACTConfig.VALID_PAYMENT_METHODS)}
                }
            },
            "allowances_charges": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["indicator", "amount"],
                    "properties": {
                        "indicator": {"type": "string", "enum": ["A", "C"]},
                        "amount": {"type": ["string", "number"], "minimum": 0},
                        "description": {"type": "string"}
                    }
                }
            }
        }
    }
    
    @staticmethod
    def sanitize_value(value: Any, uppercase: bool = False, max_length: Optional[int] = None) -> str:
        """Sanitize input value by stripping and optionally converting to uppercase."""
        if value is None:
            return ""
        result = str(value).strip()
        if uppercase:
            result = result.upper()
        if max_length and len(result) > max_length:
            result = result[:max_length]
        return result
    
    @staticmethod
    def validate_date(date_str: str, date_format: str = EDIFACTConfig.DEFAULT_DATE_FORMAT) -> bool:
        """Validate date format with enhanced checks"""
        try:
            if not re.match(r'^\d{8}$', date_str.strip()):
                return False
            datetime.datetime.strptime(date_str.strip(), date_format)
            return True
        except ValueError:
            return False
    
    @staticmethod
    def validate_decimal(value: Any) -> bool:
        """Validate decimal value"""
        try:
            Decimal(str(value).strip())
            return True
        except (InvalidOperation, TypeError):
            return False
    
    @staticmethod
    def validate_positive_number(value: Any, max_value: Optional[float] = None) -> bool:
        """Validate positive number with optional max value"""
        try:
            num = Decimal(str(value).strip())
            if num < 0:
                return False
            if max_value is not None and num > max_value:
                return False
            return True
        except (InvalidOperation, TypeError):
            return False
    
    @staticmethod
    def validate_alphanumeric(value: str, field_name: str) -> None:
        """Validate that value contains only alphanumeric characters and allowed symbols"""
        if not re.match(r'^[a-zA-Z0-9\s_\-\.\@\#\&]+$', value):
            raise EDIFACTGeneratorError(f"Invalid characters in {field_name}: {value}")
    
    @classmethod
    def validate_character_set(cls, value: str, character_set: str, field_name: str) -> None:
        """Validate that value complies with the specified character set (e.g., UNOA)"""
        if character_set == "UNOA":
            value = value.upper()
            invalid_chars = set(value) - EDIFACTConfig.UNOA_ALLOWED_CHARS
            if invalid_chars:
                raise EDIFACTGeneratorError(
                    f"Invalid characters for UNOA in {field_name}: {''.join(invalid_chars)}"
                )
    
    @classmethod
    def validate_party(cls, party: Dict[str, str], character_set: str) -> None:
        """Validate party information with enhanced validation"""
        if "qualifier" not in party or "id" not in party:
            raise EDIFACTGeneratorError("Each party must have 'qualifier' and 'id'")
        
        qualifier = cls.sanitize_value(party["qualifier"], uppercase=True)
        party_id = cls.sanitize_value(party["id"])
        
        cls.validate_character_set(qualifier, character_set, "party qualifier")
        cls.validate_character_set(party_id, character_set, "party id")
        
        if qualifier not in EDIFACTConfig.VALID_PARTY_QUALIFIERS:
            raise EDIFACTGeneratorError(
                f"Invalid party qualifier: {qualifier}. "
                f"Valid values are: {', '.join(EDIFACTConfig.VALID_PARTY_QUALIFIERS)}"
            )
        
        if not party_id:
            raise EDIFACTGeneratorError("Party ID must be a non-empty string")
        
        if "name" in party:
            name = cls.sanitize_value(party["name"], uppercase=True, max_length=EDIFACTConfig.MAX_NAME_LENGTH)
            cls.validate_alphanumeric(name, "party name")
            cls.validate_character_set(name, character_set, "party name")
        
        if "country" in party:
            country = cls.sanitize_value(party["country"], uppercase=True)
            cls.validate_character_set(country, character_set, "country")
            if country not in EDIFACTConfig.VALID_COUNTRIES:
                raise EDIFACTGeneratorError(
                    f"Invalid country code: {country}. "
                    f"Valid values are: {', '.join(EDIFACTConfig.VALID_COUNTRIES)}"
                )
    
    @classmethod
    def validate_item(cls, item: Dict[str, Any], index: int, character_set: str) -> None:
        """Validate an invoice item with enhanced validation"""
        required_fields = ["product_code", "description", "quantity", "price"]
        for field in required_fields:
            if field not in item:
                raise EDIFACTGeneratorError(f"Item {index} missing required field: {field}")
        
        product_code = cls.sanitize_value(item["product_code"], uppercase=True)
        description = cls.sanitize_value(item["description"], uppercase=True)
        
        cls.validate_alphanumeric(product_code, "product code")
        cls.validate_alphanumeric(description, "description")
        cls.validate_character_set(product_code, character_set, "product code")
        cls.validate_character_set(description, character_set, "description")
        
        if len(product_code) > EDIFACTConfig.MAX_PRODUCT_CODE_LENGTH:
            raise EDIFACTGeneratorError(
                f"Item {index} product code exceeds maximum length of "
                f"{EDIFACTConfig.MAX_PRODUCT_CODE_LENGTH} characters"
            )
        
        if len(description) > EDIFACTConfig.MAX_DESCRIPTION_LENGTH:
            raise EDIFACTGeneratorError(
                f"Item {index} description exceeds maximum length of "
                f"{EDIFACTConfig.MAX_DESCRIPTION_LENGTH} characters"
            )
        
        if not cls.validate_positive_number(item["quantity"], EDIFACTConfig.MAX_QUANTITY):
            raise EDIFACTGeneratorError(f"Item {index} has invalid quantity: {item['quantity']}")
        
        if not cls.validate_positive_number(item["price"], EDIFACTConfig.MAX_PRICE):
            raise EDIFACTGeneratorError(f"Item {index} has invalid price: {item['price']}")
        
        if "tax_rate" in item and not cls.validate_positive_number(item["tax_rate"]):
            raise EDIFACTGeneratorError(f"Item {index} has invalid tax_rate: {item['tax_rate']}")
        
        if "unit" in item:
            unit = cls.sanitize_value(item["unit"], uppercase=True)
            cls.validate_alphanumeric(unit, "unit")
            cls.validate_character_set(unit, character_set, "unit")
    
    @classmethod
    def validate_allowance_charge(cls, allowance: Dict[str, Any], index: int, character_set: str) -> None:
        """Validate an allowance or charge"""
        required_fields = ["indicator", "amount"]
        for field in required_fields:
            if field not in allowance:
                raise EDIFACTGeneratorError(f"Allowance/Charge {index} missing required field: {field}")
        
        indicator = cls.sanitize_value(allowance["indicator"], uppercase=True)
        if indicator not in {"A", "C"}:
            raise EDIFACTGeneratorError(f"Allowance/Charge {index} has invalid indicator: {indicator}")
        
        cls.validate_character_set(indicator, character_set, "allowance indicator")
        if not cls.validate_positive_number(allowance["amount"]):
            raise EDIFACTGeneratorError(f"Allowance/Charge {index} has invalid amount: {allowance['amount']}")
        
        if "description" in allowance:
            description = cls.sanitize_value(allowance["description"], uppercase=True)
            cls.validate_alphanumeric(description, "allowance description")
            cls.validate_character_set(description, character_set, "allowance description")

class EDIFACTGenerator:
    """Generates EDIFACT INVOIC messages with enhanced functionality"""
    
    def __init__(
        self, 
        logger: Optional[logging.Logger] = None,
        data_separator: str = EDIFACTConfig.DEFAULT_DATA_SEPARATOR,
        component_separator: str = EDIFACTConfig.DEFAULT_COMPONENT_SEPARATOR,
        segment_terminator: str = EDIFACTConfig.DEFAULT_SEGMENT_TERMINATOR,
        decimal_notation: str = EDIFACTConfig.DEFAULT_DECIMAL_NOTATION,
        precision: int = EDIFACTConfig.DEFAULT_PRECISION
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.data_separator = data_separator
        self.component_separator = component_separator
        self.segment_terminator = segment_terminator
        self.decimal_notation = decimal_notation
        self.precision = precision
    
    def _escape_segment_value(self, value: str) -> str:
        """Escape EDIFACT special characters in segment values"""
        value = value.replace("?", "??")
        value = value.replace("'", "?'")
        value = value.replace(self.data_separator, f"?{self.data_separator}")
        value = value.replace(self.component_separator, f"?{self.component_separator}")
        value = value.replace(" ", "? ")
        return value
    
    def _build_segment(self, segment_id: str, *elements: str) -> str:
        """Build an EDIFACT segment with proper formatting"""
        segment_elements = self.data_separator.join(elements)
        return f"{segment_id}{self.data_separator}{segment_elements}{self.segment_terminator}"
    
    def generate_invoic(
        self,
        data: Dict[str, Any],
        filename: Optional[str] = None,
        edi_version: str = EDIFACTConfig.DEFAULT_EDI_VERSION,
        character_set: str = EDIFACTConfig.DEFAULT_CHARACTER_SET,
        date_format: str = EDIFACTConfig.DEFAULT_DATE_FORMAT,
        file_encoding: str = EDIFACTConfig.DEFAULT_FILE_ENCODING,
        force: bool = False,
        interchange_control_ref: Optional[str] = None,
        application_ref: Optional[str] = None
    ) -> str:
        """
        Generate an EDIFACT INVOIC message and optionally save to a file.
        """
        if file_encoding not in EDIFACTConfig.VALID_ENCODINGS:
            raise EDIFACTGeneratorError(
                f"Invalid file encoding: {file_encoding}. "
                f"Valid encodings: {', '.join(EDIFACTConfig.VALID_ENCODINGS)}"
            )
        
        self._validate_invoice_data(data, date_format, character_set)
        self.logger.info({"message": "Generating INVOIC message", "invoice_number": data["invoice_number"]})
        
        buffer = io.StringIO()
        segments = []
        
        unb_segment = self._generate_interchange_header(
            data, 
            interchange_control_ref, 
            application_ref, 
            character_set
        )
        segments.append(unb_segment)
        self.logger.debug({"message": "Added segment", "segment": EDIFACTConfig.SEGMENT_UNB})
        
        unh_segment = self._generate_message_header(data, edi_version, character_set)
        segments.append(unh_segment)
        self.logger.debug({"message": "Added segment", "segment": EDIFACTConfig.SEGMENT_UNH})
        
        bgm_segment = self._build_segment(
            EDIFACTConfig.SEGMENT_BGM,
            EDIFACTConfig.INVOIC_DOCUMENT_TYPE,
            self._escape_segment_value(EDIFACTValidator.sanitize_value(data["invoice_number"])),
            EDIFACTConfig.ORIGINAL_DOCUMENT
        )
        segments.append(bgm_segment)
        self.logger.debug({"message": "Added segment", "segment": EDIFACTConfig.SEGMENT_BGM})
        
        dtm_segment = self._build_segment(
            EDIFACTConfig.SEGMENT_DTM,
            f"137{self.component_separator}{data['invoice_date']}{self.component_separator}102"
        )
        segments.append(dtm_segment)
        self.logger.debug({"message": "Added segment", "segment": EDIFACTConfig.SEGMENT_DTM})
        
        if "currency" in data:
            cux_segment = self._build_segment(
                EDIFACTConfig.SEGMENT_CUX,
                f"2{self.component_separator}{data['currency']}{self.component_separator}9"
            )
            segments.append(cux_segment)
            self.logger.debug({"message": "Added segment", "segment": EDIFACTConfig.SEGMENT_CUX})
        
        if "reference" in data:
            rff_segment = self._build_segment(
                EDIFACTConfig.SEGMENT_RFF,
                f"ON{self.component_separator}{self._escape_segment_value(EDIFACTValidator.sanitize_value(data['reference']))}"
            )
            segments.append(rff_segment)
            self.logger.debug({"message": "Added segment", "segment": EDIFACTConfig.SEGMENT_RFF})
        
        for segment in self._generate_party_segments(data["parties"], character_set):
            segments.append(segment)
            self.logger.debug({"message": "Added segment", "segment": EDIFACTConfig.SEGMENT_NAD})
        
        total_amount, total_tax, item_segments = self._process_items(data["items"], character_set)
        segments.extend(item_segments)
        
        if "allowances_charges" in data:
            allowance_segments = self._generate_allowance_segments(data["allowances_charges"], character_set)
            segments.extend(allowance_segments)
        
        for segment in self._generate_monetary_segments(total_amount, total_tax):
            segments.append(segment)
            self.logger.debug({"message": "Added segment", "segment": EDIFACTConfig.SEGMENT_MOA})
        
        if "payment_terms" in data:
            for segment in self._generate_payment_segments(data["payment_terms"], date_format):
                segments.append(segment)
                self.logger.debug({"message": "Added segment", "segment": segment[:3]})
        
        message_segment_count = len(segments) + 1
        unt_segment = self._build_segment(
            EDIFACTConfig.SEGMENT_UNT,
            str(message_segment_count),
            self._escape_segment_value(EDIFACTValidator.sanitize_value(data["message_ref"]))
        )
        segments.append(unt_segment)
        self.logger.debug({"message": "Added segment", "segment": EDIFACTConfig.SEGMENT_UNT})
        
        unz_segment = self._build_segment(
            EDIFACTConfig.SEGMENT_UNZ,
            "1",
            interchange_control_ref or EDIFACTValidator.sanitize_value(data["message_ref"])
        )
        segments.append(unz_segment)
        self.logger.debug({"message": "Added segment", "segment": EDIFACTConfig.SEGMENT_UNZ})
        
        edifact_message = "\n".join(segments) + "\n"
        
        if character_set == "UNOA" and not edifact_message.isascii():
            self.logger.warning({"message": "Non-ASCII characters detected with UNOA character set"})
        
        if filename:
            self._save_to_file(edifact_message, filename, file_encoding, force)
        
        return edifact_message
    
    def _generate_interchange_header(
        self, 
        data: Dict[str, Any],
        interchange_control_ref: Optional[str],
        application_ref: Optional[str],
        character_set: str
    ) -> str:
        """Generate UNB segment for interchange header"""
        now = datetime.datetime.now()
        timestamp = now.strftime("%y%m%d:%H%M")
        
        control_ref = interchange_control_ref or EDIFACTValidator.sanitize_value(data["message_ref"])
        app_ref = application_ref or "PYEDIFACT"
        
        return self._build_segment(
            EDIFACTConfig.SEGMENT_UNB,
            f"UNOC{self.component_separator}3",
            f"{app_ref}{self.component_separator}{control_ref}",
            f"RECEIVER{self.component_separator}001",
            f"{timestamp}",
            control_ref,
            f"1{self.component_separator}{character_set}"
        )
    
    def _generate_message_header(
        self, 
        data: Dict[str, Any],
        edi_version: str,
        character_set: str
    ) -> str:
        """Generate UNH segment for message header"""
        return self._build_segment(
            EDIFACTConfig.SEGMENT_UNH,
            f"{self._escape_segment_value(EDIFACTValidator.sanitize_value(data['message_ref']))}",
            f"INVOIC{self.component_separator}{edi_version}{self.component_separator}{character_set}"
        )
    
    def _validate_invoice_data(self, data: Dict[str, Any], date_format: str, character_set: str) -> None:
        """Validate the complete invoice data structure with enhanced validation"""
        try:
            validate(instance=data, schema=EDIFACTValidator.JSON_SCHEMA)
        except ValidationError as e:
            raise EDIFACTGeneratorError(f"Invalid JSON structure: {str(e)}")
        
        required_fields = {
            "message_ref": str,
            "invoice_number": str,
            "invoice_date": str,
            "parties": list,
            "items": list
        }
        
        for field, field_type in required_fields.items():
            if field not in data:
                raise EDIFACTGeneratorError(f"Missing required field: {field}")
            if not isinstance(data[field], field_type):
                raise EDIFACTGeneratorError(f"Field {field} must be {field_type.__name__}")
            if not data[field]:
                raise EDIFACTGeneratorError(f"Field {field} cannot be empty")
        
        message_ref = EDIFACTValidator.sanitize_value(data["message_ref"])
        EDIFACTValidator.validate_alphanumeric(message_ref, "message reference")
        EDIFACTValidator.validate_character_set(message_ref, character_set, "message reference")
        
        if not EDIFACTValidator.validate_date(data["invoice_date"], date_format):
            raise EDIFACTGeneratorError(f"Invalid invoice_date format. Expected {date_format}")
        
        required_qualifiers = {"BY", "SU"}
        party_qualifiers = {
            EDIFACTValidator.sanitize_value(party["qualifier"], uppercase=True) 
            for party in data["parties"]
        }
        
        missing_qualifiers = required_qualifiers - party_qualifiers
        if missing_qualifiers:
            raise EDIFACTGeneratorError(
                f"Missing required party qualifiers: {', '.join(missing_qualifiers)}"
            )
        
        if len(party_qualifiers) != len(data["parties"]):
            raise EDIFACTGeneratorError("Duplicate party qualifiers detected")
        
        for party in data["parties"]:
            EDIFACTValidator.validate_party(party, character_set)
        
        if len(data["items"]) == 0:
            raise EDIFACTGeneratorError("INVOIC must contain at least one item")
        
        for index, item in enumerate(data["items"], start=1):
            EDIFACTValidator.validate_item(item, index, character_set)
        
        if "payment_terms" in data:
            if "due_date" in data["payment_terms"]:
                if not EDIFACTValidator.validate_date(data["payment_terms"]["due_date"], date_format):
                    raise EDIFACTGeneratorError(f"Invalid due_date format. Expected {date_format}")
            if "method" in data["payment_terms"]:
                method = EDIFACTValidator.sanitize_value(data["payment_terms"]["method"])
                if method not in EDIFACTConfig.VALID_PAYMENT_METHODS:
                    raise EDIFACTGeneratorError(
                        f"Invalid payment method: {method}. "
                        f"Valid values are: {', '.join(EDIFACTConfig.VALID_PAYMENT_METHODS)}"
                    )
        
        if "currency" in data:
            currency = EDIFACTValidator.sanitize_value(data["currency"], uppercase=True)
            EDIFACTValidator.validate_character_set(currency, character_set, "currency")
            if currency not in EDIFACTConfig.VALID_CURRENCIES:
                raise EDIFACTGeneratorError(
                    f"Invalid currency: {currency}. "
                    f"Valid values are: {', '.join(EDIFACTConfig.VALID_CURRENCIES)}"
                )
        
        if "reference" in data:
            reference = EDIFACTValidator.sanitize_value(data["reference"])
            EDIFACTValidator.validate_alphanumeric(reference, "reference")
            EDIFACTValidator.validate_character_set(reference, character_set, "reference")
        
        if "allowances_charges" in data:
            for index, allowance in enumerate(data["allowances_charges"], start=1):
                EDIFACTValidator.validate_allowance_charge(allowance, index, character_set)
    
    def _generate_party_segments(self, parties: List[Dict[str, str]], character_set: str) -> List[str]:
        """Generate NAD segments for all parties with enhanced information"""
        segments = []
        
        for party in parties:
            qualifier = EDIFACTValidator.sanitize_value(party["qualifier"], uppercase=True)
            party_id = self._escape_segment_value(EDIFACTValidator.sanitize_value(party["id"]))
            
            elements = [qualifier, party_id, "91"]
            
            if "name" in party:
                name = self._escape_segment_value(
                    EDIFACTValidator.sanitize_value(
                        party["name"], 
                        uppercase=True,
                        max_length=EDIFACTConfig.MAX_NAME_LENGTH
                    )
                )
                elements.append(name)
            else:
                elements.append("")
            
            address_elements = []
            if "street" in party:
                street = self._escape_segment_value(
                    EDIFACTValidator.sanitize_value(
                        party["street"], 
                        uppercase=True,
                        max_length=EDIFACTConfig.MAX_ADDRESS_LINE_LENGTH
                    )
                )
                EDIFACTValidator.validate_character_set(street, character_set, "street")
                address_elements.append(street)
            
            if "city" in party:
                city = self._escape_segment_value(
                    EDIFACTValidator.sanitize_value(
                        party["city"], 
                        uppercase=True,
                        max_length=EDIFACTConfig.MAX_CITY_LENGTH
                    )
                )
                EDIFACTValidator.validate_character_set(city, character_set, "city")
                address_elements.append(city)
            
            if "country" in party:
                country = EDIFACTValidator.sanitize_value(
                    party["country"], 
                    uppercase=True,
                    max_length=EDIFACTConfig.MAX_COUNTRY_LENGTH
                )
                address_elements.append(country)
            
            if address_elements:
                elements.append(self.component_separator.join([e for e in address_elements if e]))
            
            segments.append(self._build_segment(EDIFACTConfig.SEGMENT_NAD, *elements))
        
        return segments
    
    def _process_items(
        self,
        items: List[Dict[str, Any]],
        character_set: str
    ) -> Tuple[Decimal, Decimal, List[str]]:
        """Process all items and generate segments, returning totals and segments"""
        total_amount = Decimal("0.00")
        total_tax = Decimal("0.00")
        segments = []
        
        for index, item in enumerate(items, start=1):
            quantity = Decimal(EDIFACTValidator.sanitize_value(item["quantity"]))
            price = Decimal(EDIFACTValidator.sanitize_value(item["price"]))
            tax_rate = Decimal(EDIFACTValidator.sanitize_value(item.get("tax_rate", "0")))
            unit = EDIFACTValidator.sanitize_value(item.get("unit", "EA"), uppercase=True)
            
            line_total = price * quantity
            total_amount += line_total
            
            lin_segment = self._build_segment(
                EDIFACTConfig.SEGMENT_LIN,
                str(index),
                "",
                f"{self._escape_segment_value(EDIFACTValidator.sanitize_value(item['product_code'], uppercase=True))}{self.component_separator}EN"
            )
            segments.append(lin_segment)
            self.logger.debug({"message": "Added segment", "segment": "LIN", "item_index": index})
            
            imd_segment = self._build_segment(
                EDIFACTConfig.SEGMENT_IMD,
                "F",
                "",
                "",
                "",
                self._escape_segment_value(EDIFACTValidator.sanitize_value(item["description"], uppercase=True))
            )
            segments.append(imd_segment)
            self.logger.debug({"message": "Added segment", "segment": "IMD", "item_index": index})
            
            qty_segment = self._build_segment(
                EDIFACTConfig.SEGMENT_QTY,
                f"47{self.component_separator}{quantity}{self.component_separator}{unit}"
            )
            segments.append(qty_segment)
            self.logger.debug({"message": "Added segment", "segment": "QTY", "item_index": index})
            
            pri_segment = self._build_segment(
                EDIFACTConfig.SEGMENT_PRI,
                f"AAA{self.component_separator}{price:.{self.precision}f}{self.component_separator}{unit}"
            )
            segments.append(pri_segment)
            self.logger.debug({"message": "Added segment", "segment": "PRI", "item_index": index})
            
            if tax_rate > 0:
                tax_value = (line_total * tax_rate) / Decimal("100")
                total_tax += tax_value
                
                tax_segment = self._build_segment(
                    EDIFACTConfig.SEGMENT_TAX,
                    "7",
                    EDIFACTConfig.VAT_TAX_CATEGORY,
                    "",
                    "",
                    f"{tax_rate:.{self.precision}f}",
                    "S"
                )
                segments.append(tax_segment)
                self.logger.debug({"message": "Added segment", "segment": "TAX", "item_index": index})
                
                moa_tax_segment = self._build_segment(
                    EDIFACTConfig.SEGMENT_MOA,
                    f"125{self.component_separator}{tax_value:.{self.precision}f}"
                )
                segments.append(moa_tax_segment)
                self.logger.debug({"message": "Added segment", "segment": "MOA", "item_index": index, "type": "tax"})
        
        return total_amount, total_tax, segments
    
    def _generate_allowance_segments(self, allowances: List[Dict[str, Any]], character_set: str) -> List[str]:
        """Generate ALC segments for allowances and charges"""
        segments = []
        for index, allowance in enumerate(allowances, start=1):
            indicator = EDIFACTValidator.sanitize_value(allowance["indicator"], uppercase=True)
            amount = Decimal(EDIFACTValidator.sanitize_value(allowance["amount"]))
            
            alc_segment = self._build_segment(
                EDIFACTConfig.SEGMENT_ALC,
                indicator,
                "",
                "",
                "",
                f"8{self.component_separator}{self._escape_segment_value(EDIFACTValidator.sanitize_value(allowance.get('description', ''), uppercase=True))}"
            )
            segments.append(alc_segment)
            self.logger.debug({"message": "Added segment", "segment": "ALC", "allowance_index": index})
            
            moa_segment = self._build_segment(
                EDIFACTConfig.SEGMENT_MOA,
                f"8{self.component_separator}{amount:.{self.precision}f}"
            )
            segments.append(moa_segment)
            self.logger.debug({"message": "Added segment", "segment": "MOA", "allowance_index": index, "type": "allowance"})
        
        return segments
    
    def _generate_monetary_segments(
        self,
        total_amount: Decimal,
        total_tax: Decimal
    ) -> List[str]:
        """Generate MOA segments for monetary totals"""
        grand_total = total_amount + total_tax
        segments = [
            self._build_segment(
                EDIFACTConfig.SEGMENT_MOA,
                f"86{self.component_separator}{total_amount:.{self.precision}f}"
            ),
            self._build_segment(
                EDIFACTConfig.SEGMENT_MOA,
                f"176{self.component_separator}{total_tax:.{self.precision}f}"
            ),
            self._build_segment(
                EDIFACTConfig.SEGMENT_MOA,
                f"9{self.component_separator}{grand_total:.{self.precision}f}"
            )
        ]
        return segments
    
    def _generate_payment_segments(self, payment_terms: Dict[str, Any], date_format: str) -> List[str]:
        """Generate PAT and DTM segments for payment terms"""
        segments = []
        if "due_date" in payment_terms:
            payment_method = EDIFACTValidator.sanitize_value(
                payment_terms.get("method", EDIFACTConfig.BANK_TRANSFER_PAYMENT)
            )
            
            pat_segment = self._build_segment(
                EDIFACTConfig.SEGMENT_PAT,
                "1",
                "",
                payment_method
            )
            segments.append(pat_segment)
            
            dtm_segment = self._build_segment(
                EDIFACTConfig.SEGMENT_DTM,
                f"13{self.component_separator}{payment_terms['due_date']}{self.component_separator}102"
            )
            segments.append(dtm_segment)
            
        return segments
    
    def _save_to_file(self, content: str, filename: str, encoding: str, force: bool = False) -> None:
        """Save the EDI message to a file with specified encoding"""
        if not force and os.path.exists(filename):
            raise EDIFACTGeneratorError(f"File {filename} exists. Use --force to overwrite.")
        try:
            with open(filename, "w", encoding=encoding) as f:
                f.write(content)
            self.logger.info({"message": "INVOIC message saved", "filename": filename, "encoding": encoding})
        except PermissionError:
            self.logger.error({"message": "Permission denied", "filename": filename})
            raise EDIFACTGeneratorError(f"Permission denied writing to {filename}")
        except OSError as e:
            self.logger.error({"message": "Failed to write file", "filename": filename, "error": str(e)})
            raise EDIFACTGeneratorError(f"File write error: {e}") from e

def configure_logging(level: int = logging.INFO) -> logging.Logger:
    """Configure application logging with structured JSON output"""
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        class JSONFormatter(logging.Formatter):
            def format(self, record):
                log_data = {
                    "timestamp": self.formatTime(record, "%Y-%m-%d %H:%M:%S"),
                    "level": record.levelname,
                    "message": record.getMessage()
                }
                if record.exc_info:
                    log_data["exception"] = self.formatException(record.exc_info)
                return json.dumps(log_data)
        
        handler = logging.StreamHandler()
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)
        logger.setLevel(level)
    return logger

def generate_example_invoic() -> Dict[str, Any]:
    """Generate example INVOIC data for testing with enhanced information"""
    return {
        "message_ref": "INV2025001",
        "invoice_number": "INV2025001",
        "invoice_date": "20250322",
        "currency": "EUR",
        "reference": "PO12345",
        "parties": [
            {
                "qualifier": "BY", 
                "id": "123456789",
                "name": "ACME CORPORATION",
                "street": "123 MAIN STREET",
                "city": "NEW YORK",
                "country": "US"
            },
            {
                "qualifier": "SU", 
                "id": "987654321",
                "name": "WIDGETS INC",
                "street": "456 INDUSTRIAL AVE",
                "city": "CHICAGO",
                "country": "US"
            },
            {
                "qualifier": "IV", 
                "id": "555555555",
                "name": "INVOICE DEPARTMENT",
                "street": "123 MAIN STREET",
                "city": "NEW YORK",
                "country": "US"
            }
        ],
        "items": [
            {
                "product_code": "ABC123",
                "description": "PREMIUM WIDGET",
                "quantity": "10",
                "price": "25.50",
                "tax_rate": "20",
                "unit": "PCE"
            },
            {
                "product_code": "XYZ456",
                "description": "DELUXE GADGET",
                "quantity": "5",
                "price": "40.00",
                "tax_rate": "20",
                "unit": "PCE"
            }
        ],
        "payment_terms": {
            "due_date": "20250422",
            "method": "5"
        },
        "allowances_charges": [
            {
                "indicator": "A",
                "amount": "10.00",
                "description": "DISCOUNT"
            }
        ]
    }

if __name__ == "__main__":
    logger = configure_logging(logging.DEBUG)
    
    parser = argparse.ArgumentParser(description="Generate EDIFACT INVOIC messages")
    parser.add_argument("--input", help="JSON file with invoice data")
    parser.add_argument("--output", default="invoic.edi", help="Output EDI file (default: invoic.edi)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output file")
    parser.add_argument("--interchange-ref", help="Interchange control reference")
    parser.add_argument("--application-ref", default="PYEDIFACT", help="Application reference")
    parser.add_argument("--character-set", default="UNOA", choices=["UNOA", "UNOB"], 
                       help="Character set (default: UNOA)")
    parser.add_argument("--precision", type=int, default=EDIFACTConfig.DEFAULT_PRECISION,
                       help="Decimal precision for monetary values (default: 2)")
    args = parser.parse_args()
    
    try:
        if args.input:
            with open(args.input, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info({"message": "Loaded invoice data", "filename": args.input})
        else:
            data = generate_example_invoic()
            logger.info({"message": "Using example invoice data"})
        
        generator = EDIFACTGenerator(logger=logger, precision=args.precision)
        edi_message = generator.generate_invoic(
            data,
            filename=args.output,
            character_set=args.character_set,
            force=args.force,
            interchange_control_ref=args.interchange_ref,
            application_ref=args.application_ref
        )
        
        print("\nGenerated INVOIC Message:\n")
        print(edi_message)
        print(f"\nInvoice saved to '{args.output}'")
    except (EDIFACTGeneratorError, OSError, json.JSONDecodeError, ValidationError) as e:
        logger.error({"message": "Failed to generate INVOIC", "error": str(e)})
        exit(1)
