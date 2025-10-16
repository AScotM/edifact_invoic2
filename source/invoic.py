import json
import logging
import re
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional
from datetime import datetime
import uuid
from jsonschema import validate, ValidationError
import os

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

class EDIFACTBaseError(Exception):
    pass

class EDIFACTValidationError(EDIFACTBaseError):
    pass

class EDIFACTGenerationError(EDIFACTBaseError):
    pass

class EDIFACTConfig:
    SUPPORTED_CHARSETS = {"UNOA", "UNOB", "UNOC"}
    SUPPORTED_CURRENCIES = {"EUR", "USD", "GBP", "JPY", "CAD"}
    SUPPORTED_DATE_FORMATS = {"102", "203", "101"}
    MAX_PARTY_ID_LENGTH = 35
    MAX_NAME_LENGTH = 70
    MAX_ITEM_ID_LENGTH = 35
    MAX_TEXT_LENGTH = 350

class EDIFACTValidator:
    JSON_SCHEMA = {
        "type": "object",
        "properties": {
            "charset": {"type": "string"},
            "invoice_number": {"type": "string"},
            "invoice_date": {"type": "string"},
            "currency": {"type": "string"},
            "due_date": {"type": "string"},
            "tax_rate": {"type": "number"},
            "parties": {
                "type": "object",
                "properties": {
                    "buyer": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "name": {"type": "string"},
                            "address": {"type": "string"},
                            "contact": {"type": "string"}
                        },
                        "required": ["id"]
                    },
                    "seller": {
                        "type": "object",
                        "properties": {
                            "id": {"type": "string"},
                            "name": {"type": "string"},
                            "address": {"type": "string"},
                            "contact": {"type": "string"}
                        },
                        "required": ["id"]
                    }
                },
                "required": ["buyer", "seller"]
            },
            "items": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "description": {"type": "string"},
                        "quantity": {"type": "number"},
                        "price": {"type": "number"},
                        "unit": {"type": "string"}
                    },
                    "required": ["id", "quantity", "price"]
                }
            },
            "payment_terms": {"type": "string"}
        },
        "required": ["invoice_number", "invoice_date", "currency", "parties", "items"]
    }

    @classmethod
    def validate_schema(cls, data: Dict[str, Any]) -> None:
        try:
            validate(instance=data, schema=cls.JSON_SCHEMA)
        except ValidationError as e:
            raise EDIFACTValidationError(f"Schema validation failed: {e.message}")

    @classmethod
    def validate_fields(cls, data: Dict[str, Any]) -> None:
        if data.get("charset") and data["charset"] not in EDIFACTConfig.SUPPORTED_CHARSETS:
            raise EDIFACTValidationError(f"Unsupported charset: {data['charset']}")
        
        if data["currency"] not in EDIFACTConfig.SUPPORTED_CURRENCIES:
            raise EDIFACTValidationError(f"Unsupported currency: {data['currency']}")
        
        cls._validate_date(data["invoice_date"], "invoice_date")
        if data.get("due_date"):
            cls._validate_date(data["due_date"], "due_date")
        
        for party in ("buyer", "seller"):
            cls._validate_party(data["parties"][party], party)
        
        for idx, item in enumerate(data["items"]):
            cls._validate_item(item, idx)

    @classmethod
    def _validate_date(cls, date_str: str, field_name: str) -> None:
        if not re.match(r"^\d{8}$", date_str):
            raise EDIFACTValidationError(f"{field_name} must be in YYYYMMDD format")
        try:
            datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            raise EDIFACTValidationError(f"Invalid date in {field_name}: {date_str}")

    @classmethod
    def _validate_party(cls, party: Dict[str, Any], role: str) -> None:
        if not party.get("id"):
            raise EDIFACTValidationError(f"{role} ID is required")
        
        if len(party["id"]) > EDIFACTConfig.MAX_PARTY_ID_LENGTH:
            raise EDIFACTValidationError(f"{role} ID too long: {len(party['id'])} > {EDIFACTConfig.MAX_PARTY_ID_LENGTH}")
        
        if party.get("name") and len(party["name"]) > EDIFACTConfig.MAX_NAME_LENGTH:
            raise EDIFACTValidationError(f"{role} name too long")

    @classmethod
    def _validate_item(cls, item: Dict[str, Any], index: int) -> None:
        if len(item["id"]) > EDIFACTConfig.MAX_ITEM_ID_LENGTH:
            raise EDIFACTValidationError(f"Item {index} ID too long")
        
        if item["quantity"] <= 0:
            raise EDIFACTValidationError(f"Item {index} quantity must be positive")
        
        if item["price"] <= 0:
            raise EDIFACTValidationError(f"Item {index} price must be positive")

class EDIFACTGenerator:
    def __init__(self, data: Dict[str, Any], precision: int = 2, line_ending: str = "\n"):
        self.data = data
        self.precision = precision
        self.line_ending = line_ending
        self.message_ref = data.get("message_ref") or str(uuid.uuid4().int)[:14]
        self.segments: List[str] = []

    def _format_decimal(self, value: Any) -> str:
        d = Decimal(str(value)).quantize(
            Decimal(f"1.{'0'*self.precision}"), rounding=ROUND_HALF_UP
        )
        return f"{d:.{self.precision}f}"

    def _escape_segment_value(self, value: Any) -> str:
        if value is None:
            return ""
        s = str(value)
        s = s.replace("?", "??")
        for char in ["'", "+", ":", "*"]:
            s = s.replace(char, f"?{char}")
        return s

    def _build_segment(self, tag: str, elements: List[Any]) -> str:
        escaped_elements = [self._escape_segment_value(e) for e in elements]
        return "+".join([tag] + escaped_elements) + "'"

    def _add_una_segment(self) -> None:
        self.segments.append("UNA:+.? '")

    def _add_unb_segment(self) -> None:
        timestamp = datetime.now().strftime("%y%m%d%H%M")
        sender_id = self.data.get("sender_id", "SENDER")
        receiver_id = self.data.get("receiver_id", "RECEIVER")
        self.segments.append(
            self._build_segment("UNB", ["UNOC", "3", sender_id, receiver_id, timestamp, self.message_ref])
        )

    def _add_unz_segment(self, segment_count: int) -> None:
        self.segments.append(self._build_segment("UNZ", [segment_count, self.message_ref]))

    def _add_header_segments(self) -> None:
        self.segments.append(
            self._build_segment("UNH", [self.message_ref, "INVOIC:D:96A:UN"])
        )
        self.segments.append(
            self._build_segment("BGM", ["380", self.data["invoice_number"], "9"])
        )
        self.segments.append(
            self._build_segment("DTM", ["137", self.data["invoice_date"], "102"])
        )
        
        if self.data.get("due_date"):
            self.segments.append(
                self._build_segment("DTM", ["13", self.data["due_date"], "102"])
            )

    def _add_currency_segment(self) -> None:
        self.segments.append(
            self._build_segment("CUX", ["2", self.data["currency"], "9"])
        )

    def _add_party_segments(self) -> None:
        for role, code in {"buyer": "BY", "seller": "SE"}.items():
            party = self.data["parties"][role]
            self.segments.append(
                self._build_segment("NAD", [code, party["id"], "", "91", party.get("name", "")])
            )
            
            if party.get("address"):
                self.segments.append(
                    self._build_segment("LOC", ["11", party["address"]])
                )
            
            if party.get("contact"):
                self.segments.append(
                    self._build_segment("COM", [party["contact"], "TE"])
                )

    def _add_line_items(self) -> None:
        for idx, item in enumerate(self.data["items"], start=1):
            self.segments.append(
                self._build_segment("LIN", [str(idx), "", item["id"], "EN"])
            )
            
            if item.get("description"):
                self.segments.append(
                    self._build_segment("IMD", ["F", "", "", "", item["description"]])
                )
            
            unit = item.get("unit", "PCE")
            self.segments.append(
                self._build_segment("QTY", ["47", self._format_decimal(item["quantity"]), unit])
            )
            
            self.segments.append(
                self._build_segment("PRI", ["AAA", self._format_decimal(item["price"]), unit])
            )

    def _add_summary_segments(self) -> None:
        subtotal = sum(
            Decimal(str(item["quantity"])) * Decimal(str(item["price"])) 
            for item in self.data["items"]
        )
        
        self.segments.append(
            self._build_segment("MOA", ["79", self._format_decimal(subtotal)])
        )
        
        if self.data.get("tax_rate"):
            tax_rate = Decimal(str(self.data["tax_rate"]))
            tax_amount = (subtotal * tax_rate / Decimal("100")).quantize(
                Decimal(f"1.{'0'*self.precision}"), rounding=ROUND_HALF_UP
            )
            self.segments.append(
                self._build_segment("TAX", ["7", "VAT", "", "", "", "", self._format_decimal(tax_rate)])
            )
            self.segments.append(
                self._build_segment("MOA", ["124", self._format_decimal(tax_amount)])
            )
            subtotal += tax_amount
        
        if self.data.get("payment_terms"):
            self.segments.append(
                self._build_segment("PAI", [self.data["payment_terms"], "3"])
            )

    def _add_unt_segment(self) -> None:
        unh_index = next(i for i, s in enumerate(self.segments) if s.startswith("UNH+"))
        segment_count = len(self.segments) - unh_index
        self.segments.append(
            self._build_segment("UNT", [str(segment_count), self.message_ref])
        )

    def generate(self) -> str:
        logger.info("Validating JSON input")
        EDIFACTValidator.validate_schema(self.data)
        EDIFACTValidator.validate_fields(self.data)

        logger.info("Generating EDIFACT segments")
        self.segments = []

        self._add_una_segment()
        self._add_unb_segment()
        self._add_header_segments()
        self._add_currency_segment()
        self._add_party_segments()
        self._add_line_items()
        self._add_summary_segments()
        self._add_unt_segment()
        self._add_unz_segment(len(self.segments))

        return self.line_ending.join(self.segments)

    def save_to_file(self, filename: Optional[str] = None) -> str:
        message = self.generate()
        if not filename:
            filename = f"invoice_{self.data['invoice_number']}.edi"
        with open(filename, "w", encoding="utf-8", newline="") as f:
            f.write(message)
        logger.info(f"EDIFACT INVOIC saved to {os.path.abspath(filename)}")
        return filename

if __name__ == "__main__":
    example_invoice = {
        "invoice_number": "INV12345",
        "invoice_date": "20250509",
        "due_date": "20250609",
        "currency": "EUR",
        "tax_rate": 21.0,
        "payment_terms": "NET30",
        "sender_id": "COMPANY_A",
        "receiver_id": "COMPANY_B",
        "parties": {
            "buyer": {
                "id": "BUYER123",
                "name": "Buyer Corporation",
                "address": "123 Main St",
                "contact": "buyer@example.com"
            },
            "seller": {
                "id": "SELLER456", 
                "name": "Seller Ltd",
                "address": "456 Oak Ave",
                "contact": "sales@seller.com"
            },
        },
        "items": [
            {
                "id": "ITEM001",
                "description": "Premium Widget",
                "quantity": 10,
                "price": 25.50,
                "unit": "PCE"
            },
            {
                "id": "ITEM002",
                "description": "Standard Widget",
                "quantity": 5,
                "price": 15.75,
                "unit": "PCE"
            },
        ],
    }

    try:
        generator = EDIFACTGenerator(example_invoice, line_ending="\r\n")
        filepath = generator.save_to_file()
        print(f"EDIFACT file generated: {filepath}")
        
        with open(filepath, 'r') as f:
            print("\nGenerated EDIFACT content:")
            print(f.read())
            
    except EDIFACTBaseError as e:
        logger.error(f"EDIFACT generation failed: {e}")
