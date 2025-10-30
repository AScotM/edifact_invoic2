import json
import logging
import re
import os
import uuid
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Any, Dict, List, Optional
from datetime import datetime
from jsonschema import validate, ValidationError

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
    SEGMENT_TERMINATOR = "'"
    DATA_ELEMENT_SEPARATOR = "+"
    COMPONENT_SEPARATOR = ":"
    MAX_SEGMENT_LENGTH = 2000
    
    @classmethod
    def configure(cls, **kwargs):
        for key, value in kwargs.items():
            if hasattr(cls, key):
                setattr(cls, key, value)

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
            "payment_terms": {"type": "string"},
            "notes": {"type": "string"},
            "bank_account": {
                "type": "object",
                "properties": {
                    "account": {"type": "string"},
                    "bank_code": {"type": "string"}
                }
            },
            "sender_id": {"type": "string"},
            "receiver_id": {"type": "string"},
            "message_ref": {"type": "string"}
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
        
        cls._validate_interdependencies(data)

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

    @classmethod
    def _validate_interdependencies(cls, data: Dict[str, Any]) -> None:
        if data.get("due_date"):
            invoice_date = datetime.strptime(data["invoice_date"], "%Y%m%d")
            due_date = datetime.strptime(data["due_date"], "%Y%m%d")
            if due_date <= invoice_date:
                raise EDIFACTValidationError("Due date must be after invoice date")
        
        item_ids = [item["id"] for item in data["items"]]
        if len(item_ids) != len(set(item_ids)):
            raise EDIFACTValidationError("Item IDs must be unique")

class EDIFACTGenerator:
    def __init__(self, data: Dict[str, Any], precision: int = 2, line_ending: str = "\n"):
        self.data = self._sanitize_input(data)
        self.precision = precision
        self.line_ending = line_ending
        self.message_ref = data.get("message_ref") or str(uuid.uuid4().int)[:14]
        self.segments: List[str] = []

    def _sanitize_input(self, data: Dict[str, Any]) -> Dict[str, Any]:
        sanitized = {}
        for key, value in data.items():
            if isinstance(value, str):
                sanitized[key] = re.sub(r'[\x00-\x1F\x7F]', '', value)
            elif isinstance(value, dict):
                sanitized[key] = self._sanitize_input(value)
            elif isinstance(value, list):
                sanitized[key] = [self._sanitize_input(item) if isinstance(item, dict) else item for item in value]
            else:
                sanitized[key] = value
        return sanitized

    def _format_decimal(self, value: Any) -> str:
        try:
            if isinstance(value, float) and (abs(value) > 1e15 or abs(value) < 1e-15):
                d = Decimal(str(value))
            else:
                d = Decimal(value)
            
            formatted = f"{d:.{self.precision}f}"
            if '.' in formatted:
                formatted = formatted.rstrip('0').rstrip('.')
            return formatted
        except (ValueError, TypeError, InvalidOperation) as e:
            raise EDIFACTGenerationError(f"Invalid numeric value: {value} - {e}")

    def _escape_segment_value(self, value: Any) -> str:
        if value is None:
            return ""
        s = str(value)
        s = s.replace("?", "??")
        for char in ["'", "+", ":", "*"]:
            s = s.replace(char, f"?{char}")
        return s

    def _validate_segment(self, tag: str, elements: List[Any]) -> None:
        if tag == "LIN" and len(elements) < 3:
            raise EDIFACTGenerationError(f"LIN segment requires at least 3 elements")
        if tag == "UNH" and len(elements) < 2:
            raise EDIFACTGenerationError(f"UNH segment requires at least 2 elements")

    def _build_segment(self, tag: str, elements: List[Any]) -> str:
        if not elements:
            elements = []
        
        self._validate_segment(tag, elements)
        
        escaped_elements = [self._escape_segment_value(e) for e in elements]
        segment = "+".join([tag] + escaped_elements) + "'"
        
        if len(segment) > EDIFACTConfig.MAX_SEGMENT_LENGTH:
            logger.warning(f"Segment {tag} exceeds recommended length: {len(segment)}")
        
        return segment

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

    def _add_ftx_segments(self) -> None:
        if self.data.get("notes"):
            notes = self.data["notes"]
            chunks = [notes[i:i+70] for i in range(0, len(notes), 70)]
            for i, chunk in enumerate(chunks, 1):
                self.segments.append(
                    self._build_segment("FTX", ["AAI", str(i), "", "", chunk])
                )

    def _add_payment_instructions(self) -> None:
        if self.data.get("bank_account"):
            bank_data = self.data["bank_account"]
            self.segments.append(
                self._build_segment("FII", ["BE", "", bank_data.get("account"), "", bank_data.get("bank_code")])
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
        self._add_ftx_segments()
        self._add_payment_instructions()
        self._add_summary_segments()
        self._add_unt_segment()
        self._add_unz_segment(len(self.segments))

        edifact_content = self.line_ending.join(self.segments)
        
        if not self.validate_edifact_syntax(edifact_content):
            raise EDIFACTGenerationError("Generated EDIFACT content failed syntax validation")
            
        return edifact_content

    def validate_edifact_syntax(self, content: str) -> bool:
        lines = content.split(self.line_ending)
        if not lines[0].startswith("UNA"):
            return False
        
        for line in lines[1:]:
            if not line.endswith("'"):
                return False
            if "??" in line and "?" in line.replace("??", ""):
                return False
        
        return True

    def _validate_file_path(self, filename: str) -> None:
        if not filename:
            return
        
        safe_filename = os.path.basename(filename)
        if safe_filename != filename:
            raise EDIFACTGenerationError("Invalid filename provided")
        
        if not filename.lower().endswith(('.edi', '.edifact')):
            logger.warning("Recommended file extension is .edi or .edifact")

    def save_to_file(self, filename: Optional[str] = None) -> str:
        message = self.generate()
        if not filename:
            filename = f"invoice_{self.data['invoice_number']}.edi"
        
        self._validate_file_path(filename)
        
        with open(filename, "w", encoding="utf-8", newline="") as f:
            f.write(message)
        logger.info(f"EDIFACT INVOIC saved to {os.path.abspath(filename)}")
        return filename

    @classmethod
    def from_json_file(cls, filepath: str, **kwargs) -> 'EDIFACTGenerator':
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return cls(data, **kwargs)

    def to_dict(self) -> Dict[str, Any]:
        return self.data.copy()

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
        "notes": "Thank you for your business. Please note that payments should be made within 30 days.",
        "bank_account": {
            "account": "NL91ABNA0417164300",
            "bank_code": "ABNANL2A"
        },
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
