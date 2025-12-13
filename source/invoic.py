import json
import logging
import re
import os
import uuid
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Any, Dict, List, Optional
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

CONTROL_CHAR_REGEX = re.compile(r'[\x00-\x1F\x7F]')
ESCAPE_CHARS = ["'", "+", ":", "*", "?"]

DATE_FORMATS = {
    "102": "%Y%m%d",
    "203": "%Y%m%d%H%M",
    "101": "%y%m%d"
}

class EDIFACTBaseError(Exception):
    pass

class EDIFACTValidationError(EDIFACTBaseError):
    def __init__(self, message: str, code: str = "VALID_001", details: Optional[Dict] = None):
        self.code = code
        self.details = details or {}
        super().__init__(f"{code}: {message}")

class EDIFACTGenerationError(EDIFACTBaseError):
    def __init__(self, message: str, code: str = "GEN_001", details: Optional[Dict] = None):
        self.code = code
        self.details = details or {}
        super().__init__(f"{code}: {message}")

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
    REPETITION_SEPARATOR = "*"
    DECIMAL_NOTATION = "."
    RELEASE_CHARACTER = "?"
    MAX_SEGMENT_LENGTH = 2000
    DEFAULT_PRECISION = 2
    DEFAULT_VERSION = "D"
    DEFAULT_RELEASE = "96A"
    
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)

class EDIFACTValidator:
    @classmethod
    def validate_schema(cls, data: Dict[str, Any]) -> None:
        required_fields = ["invoice_number", "invoice_date", "currency", "parties", "items"]
        
        for field in required_fields:
            if field not in data:
                raise EDIFACTValidationError(
                    f"Missing required field: {field}",
                    "SCHEMA_001",
                    {"missing_field": field}
                )
        
        cls._validate_field_length("invoice_number", str(data.get("invoice_number", "")), 35)
        
        if data.get("currency"):
            if len(str(data["currency"])) > 3:
                raise EDIFACTValidationError(
                    "Currency code must be 3 characters",
                    "SCHEMA_002",
                    {"currency": data["currency"]}
                )
        
        if not isinstance(data.get("parties"), dict) or "buyer" not in data.get("parties", {}) or "seller" not in data.get("parties", {}):
            raise EDIFACTValidationError(
                "Both buyer and seller parties are required",
                "SCHEMA_003"
            )
        
        for party in ("buyer", "seller"):
            if not isinstance(data["parties"].get(party), dict):
                raise EDIFACTValidationError(
                    f"{party} must be an object",
                    "SCHEMA_004",
                    {"party": party}
                )
            
            if "id" not in data["parties"][party]:
                raise EDIFACTValidationError(
                    f"{party} ID is required",
                    "SCHEMA_005",
                    {"party": party}
                )
            
            cls._validate_field_length("id", str(data["parties"][party].get("id", "")), EDIFACTConfig.MAX_PARTY_ID_LENGTH)
            
            if data["parties"][party].get("name"):
                cls._validate_field_length("name", str(data["parties"][party]["name"]), EDIFACTConfig.MAX_NAME_LENGTH)
        
        if not isinstance(data.get("items"), list) or len(data.get("items", [])) < 1:
            raise EDIFACTValidationError(
                "At least one item is required",
                "SCHEMA_006",
                {"items_count": len(data.get("items", []))}
            )
        
        for idx, item in enumerate(data.get("items", [])):
            if not isinstance(item, dict):
                raise EDIFACTValidationError(
                    f"Item {idx} must be an object",
                    "SCHEMA_007",
                    {"item_index": idx}
                )
            
            if "id" not in item or "quantity" not in item or "price" not in item:
                raise EDIFACTValidationError(
                    f"Item {idx} must contain id, quantity, and price",
                    "SCHEMA_008",
                    {"item_index": idx}
                )
            
            cls._validate_field_length("id", str(item.get("id", "")), EDIFACTConfig.MAX_ITEM_ID_LENGTH)
        
        if data.get("notes"):
            cls._validate_field_length("notes", str(data["notes"]), EDIFACTConfig.MAX_TEXT_LENGTH)

    @classmethod
    def _validate_field_length(cls, field_name: str, value: str, max_length: int) -> None:
        if len(value) > max_length:
            raise EDIFACTValidationError(
                f"Field '{field_name}' exceeds maximum length of {max_length}",
                "SCHEMA_009",
                {"field": field_name, "value": value[:50], "length": len(value)}
            )

    @classmethod
    def validate_fields(cls, data: Dict[str, Any], config: EDIFACTConfig) -> None:
        if data.get("charset") and data["charset"] not in config.SUPPORTED_CHARSETS:
            raise EDIFACTValidationError(f"Unsupported charset: {data['charset']}", "VALID_002")
        
        if data["currency"] not in config.SUPPORTED_CURRENCIES:
            raise EDIFACTValidationError(f"Unsupported currency: {data['currency']}", "VALID_003")
        
        cls._validate_date(data["invoice_date"], "invoice_date", "102")
        if data.get("due_date"):
            cls._validate_date(data["due_date"], "due_date", "102")
        
        for party in ("buyer", "seller"):
            cls._validate_party(data["parties"][party], party, config)
        
        for idx, item in enumerate(data["items"]):
            cls._validate_item(item, idx)
        
        cls._validate_interdependencies(data)

    @classmethod
    def _validate_date(cls, date_str: str, field_name: str, date_format: str) -> None:
        fmt = DATE_FORMATS.get(date_format)
        if not fmt:
            raise EDIFACTValidationError(f"Unsupported date format: {date_format}", "VALID_004")
        
        try:
            datetime.strptime(date_str, fmt)
        except ValueError:
            raise EDIFACTValidationError(f"Invalid date in {field_name}: {date_str}", "VALID_005")

    @classmethod
    def _validate_party(cls, party: Dict[str, Any], role: str, config: EDIFACTConfig) -> None:
        if not party.get("id"):
            raise EDIFACTValidationError(f"{role} ID is required", "VALID_006")
        
        if len(party["id"]) > config.MAX_PARTY_ID_LENGTH:
            raise EDIFACTValidationError(
                f"{role} ID too long: {len(party['id'])} > {config.MAX_PARTY_ID_LENGTH}",
                "VALID_007",
                {"role": role, "length": len(party["id"])}
            )
        
        if party.get("name") and len(party["name"]) > config.MAX_NAME_LENGTH:
            raise EDIFACTValidationError(
                f"{role} name too long: {len(party['name'])} > {config.MAX_NAME_LENGTH}",
                "VALID_008",
                {"role": role, "length": len(party["name"])}
            )

    @classmethod
    def _validate_item(cls, item: Dict[str, Any], index: int) -> None:
        if len(item["id"]) > EDIFACTConfig.MAX_ITEM_ID_LENGTH:
            raise EDIFACTValidationError(
                f"Item {index} ID too long: {len(item['id'])} > {EDIFACTConfig.MAX_ITEM_ID_LENGTH}",
                "VALID_009",
                {"item_index": index, "length": len(item["id"])}
            )
        
        quantity = Decimal(str(item["quantity"]))
        if quantity <= Decimal("0"):
            raise EDIFACTValidationError(
                f"Item {index} quantity must be positive",
                "VALID_010",
                {"item_index": index, "quantity": item["quantity"]}
            )
        
        price = Decimal(str(item["price"]))
        if price < Decimal("0"):
            raise EDIFACTValidationError(
                f"Item {index} price must be non-negative",
                "VALID_011",
                {"item_index": index, "price": item["price"]}
            )

    @classmethod
    def _validate_interdependencies(cls, data: Dict[str, Any]) -> None:
        if data.get("due_date"):
            invoice_date = datetime.strptime(data["invoice_date"], "%Y%m%d")
            due_date = datetime.strptime(data["due_date"], "%Y%m%d")
            if due_date <= invoice_date:
                raise EDIFACTValidationError("Due date must be after invoice date", "VALID_012")
        
        item_ids = [item["id"] for item in data["items"]]
        if len(item_ids) != len(set(item_ids)):
            raise EDIFACTValidationError("Item IDs must be unique", "VALID_013")

class EDIFACTGenerator:
    def __init__(self, data: Dict[str, Any], config: Optional[EDIFACTConfig] = None, line_ending: str = "\n"):
        self.data = self._sanitize_input(data)
        self.config = config or EDIFACTConfig()
        self.line_ending = line_ending
        self.message_ref = data.get("message_ref") or str(uuid.uuid4().int)[:14]
        self.interchange_ref = data.get("interchange_ref") or str(uuid.uuid4().int)[:14]
        self.segments: List[str] = []
        self._generated = False

    def _sanitize_input(self, data: Dict[str, Any]) -> Dict[str, Any]:
        sanitized = {}
        for key, value in data.items():
            if isinstance(value, str):
                sanitized[key] = CONTROL_CHAR_REGEX.sub('', value)
            elif isinstance(value, dict):
                sanitized[key] = self._sanitize_input(value)
            elif isinstance(value, list):
                sanitized[key] = [self._sanitize_input(item) if isinstance(item, dict) else item for item in value]
            else:
                sanitized[key] = value
        return sanitized

    def _format_decimal(self, value: Any) -> str:
        try:
            if isinstance(value, (int, float)):
                d = Decimal(str(value))
            else:
                d = Decimal(value)
            
            quantized = d.quantize(Decimal(f"1.{'0'*self.config.DEFAULT_PRECISION}"), rounding=ROUND_HALF_UP)
            
            formatted = f"{quantized:.{self.config.DEFAULT_PRECISION}f}"
            
            if self.data.get("charset") in ["UNOA", "UNOB"]:
                formatted = formatted.replace('.', ',')
            
            return formatted
        except (ValueError, TypeError, InvalidOperation) as e:
            raise EDIFACTGenerationError(f"Invalid numeric value: {value}", "GEN_003", {"error": str(e)})

    def _escape_segment_value(self, value: Any) -> str:
        if value is None:
            return ""
        
        result = []
        for char in str(value):
            if CONTROL_CHAR_REGEX.match(char):
                continue
            elif char == self.config.RELEASE_CHARACTER:
                result.extend([self.config.RELEASE_CHARACTER, self.config.RELEASE_CHARACTER])
            elif char in [self.config.SEGMENT_TERMINATOR, 
                         self.config.DATA_ELEMENT_SEPARATOR,
                         self.config.COMPONENT_SEPARATOR,
                         self.config.REPETITION_SEPARATOR,
                         self.config.RELEASE_CHARACTER]:
                result.extend([self.config.RELEASE_CHARACTER, char])
            else:
                result.append(char)
        return ''.join(result)

    def _validate_segment_length(self, segment: str) -> None:
        if len(segment) > self.config.MAX_SEGMENT_LENGTH:
            raise EDIFACTGenerationError(
                f"Segment too long: {len(segment)} > {self.config.MAX_SEGMENT_LENGTH}",
                "GEN_004",
                {"segment": segment[:100], "length": len(segment)}
            )

    def _build_segment(self, tag: str, elements: List[Any]) -> str:
        if not elements:
            elements = []
        
        escaped_elements = [self._escape_segment_value(e) for e in elements]
        segment = self.config.DATA_ELEMENT_SEPARATOR.join([tag] + escaped_elements) + self.config.SEGMENT_TERMINATOR
        
        self._validate_segment_length(segment)
        return segment

    def _add_una_segment(self) -> None:
        component_sep = self.config.COMPONENT_SEPARATOR
        data_sep = self.config.DATA_ELEMENT_SEPARATOR
        decimal_char = self.config.DECIMAL_NOTATION
        release_char = self.config.RELEASE_CHARACTER
        reserved_char = " "
        segment_term = self.config.SEGMENT_TERMINATOR
        
        una_segment = f"UNA{component_sep}{data_sep}{decimal_char}{release_char}{reserved_char}{segment_term}"
        self.segments.append(una_segment)

    def _add_unb_segment(self) -> None:
        timestamp = datetime.now().strftime("%y%m%d%H%M")
        sender_id = self.data.get("sender_id", "SENDER")
        receiver_id = self.data.get("receiver_id", "RECEIVER")
        charset = self.data.get("charset", "UNOC")
        version = self.data.get("version", self.config.DEFAULT_VERSION)
        application_ref = self.data.get("application_ref", "")
        priority = self.data.get("priority", "")
        ack_request = self.data.get("ack_request", "0")
        agreement_id = self.data.get("agreement_id", "")
        test_indicator = self.data.get("test_indicator", "1")
        
        self.segments.append(
            self._build_segment("UNB", [
                f"{charset}:{version}",
                sender_id,
                receiver_id,
                timestamp,
                self.interchange_ref,
                application_ref,
                priority,
                ack_request,
                agreement_id,
                test_indicator
            ])
        )

    def _add_unz_segment(self) -> None:
        group_count = "1"
        self.segments.append(self._build_segment("UNZ", [group_count, self.interchange_ref]))

    def _add_header_segments(self) -> None:
        self.segments.append(
            self._build_segment("UNH", [
                self.message_ref, 
                f"INVOIC:{self.config.DEFAULT_VERSION}:{self.config.DEFAULT_RELEASE}:UN"
            ])
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
        
        if self.data.get("payment_terms"):
            self.segments.append(
                self._build_segment("PAI", [self.data["payment_terms"], "3"])
            )
            
            if self.data.get("payment_due_date"):
                self.segments.append(
                    self._build_segment("DTM", ["12", self.data["payment_due_date"], "102"])
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
            
            if item.get("tax_category"):
                self.segments.append(
                    self._build_segment("TAX", ["7", item["tax_category"], "", "", "", "", ""])
                )

    def _add_ftx_segments(self) -> None:
        if self.data.get("notes"):
            notes = self.data["notes"]
            max_length = 70
            chunks = [notes[i:i+max_length] for i in range(0, len(notes), max_length)]
            for i, chunk in enumerate(chunks, 1):
                self.segments.append(
                    self._build_segment("FTX", ["AAI", str(i), "", "", chunk])
                )

    def _add_payment_instructions(self) -> None:
        if self.data.get("bank_account"):
            bank_data = self.data["bank_account"]
            if bank_data.get("account") and bank_data.get("bank_code"):
                self.segments.append(
                    self._build_segment("FII", ["BE", "", bank_data.get("account"), "", bank_data.get("bank_code")])
                )

    def _add_summary_segments(self) -> None:
        subtotal = Decimal("0.00")
        for item in self.data["items"]:
            quantity = Decimal(str(item["quantity"]))
            price = Decimal(str(item["price"]))
            subtotal += quantity * price

        subtotal_quantized = subtotal.quantize(Decimal(f"1.{'0'*self.config.DEFAULT_PRECISION}"), rounding=ROUND_HALF_UP)
        
        self.segments.append(
            self._build_segment("MOA", ["79", self._format_decimal(subtotal_quantized)])
        )
        
        if self.data.get("tax_rate"):
            tax_rate = Decimal(str(self.data["tax_rate"]))
            tax_amount = (subtotal * tax_rate / Decimal("100")).quantize(
                Decimal(f"1.{'0'*self.config.DEFAULT_PRECISION}"), rounding=ROUND_HALF_UP
            )
            self.segments.append(
                self._build_segment("TAX", ["7", "VAT", "", "", "", "", self._format_decimal(tax_rate)])
            )
            self.segments.append(
                self._build_segment("MOA", ["124", self._format_decimal(tax_amount)])
            )
            total_amount = subtotal_quantized + tax_amount
            self.segments.append(
                self._build_segment("MOA", ["86", self._format_decimal(total_amount)])
            )
        else:
            self.segments.append(
                self._build_segment("MOA", ["86", self._format_decimal(subtotal_quantized)])
            )

    def _add_unt_segment(self) -> None:
        unh_indices = [i for i, s in enumerate(self.segments) if s.startswith("UNH+")]
        if not unh_indices:
            raise EDIFACTGenerationError("UNH segment not found", "GEN_005")
        
        unh_index = unh_indices[0]
        segment_count = len(self.segments) - unh_index + 1
        self.segments.append(
            self._build_segment("UNT", [str(segment_count), self.message_ref])
        )

    def generate(self) -> str:
        if self._generated:
            return self.line_ending.join(self.segments)
            
        logger.info(f"Starting EDIFACT generation for invoice {self.data.get('invoice_number', 'Unknown')}")
        EDIFACTValidator.validate_schema(self.data)
        EDIFACTValidator.validate_fields(self.data, self.config)

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
        self._add_unz_segment()

        edifact_content = self.line_ending.join(self.segments)
        
        logger.debug(f"Generated {len(self.segments)} segments")
        
        if not self.validate_edifact_syntax(edifact_content):
            raise EDIFACTGenerationError("Generated EDIFACT content failed syntax validation", "GEN_006")
        
        self._generated = True
        return edifact_content

    def validate_edifact_syntax(self, content: str) -> bool:
        lines = content.split(self.line_ending)
        if not lines[0].startswith("UNA"):
            logger.error("Missing UNA segment")
            return False
        
        for i, line in enumerate(lines[1:], 1):
            if not line.endswith(self.config.SEGMENT_TERMINATOR):
                logger.error(f"Line {i} missing segment terminator: {line[:50]}")
                return False
            
            if len(line) > self.config.MAX_SEGMENT_LENGTH:
                logger.error(f"Line {i} exceeds max length: {len(line)} > {self.config.MAX_SEGMENT_LENGTH}")
                return False
        
        unh_count = sum(1 for line in lines if line.startswith("UNH+"))
        unt_count = sum(1 for line in lines if line.startswith("UNT+"))
        unb_count = sum(1 for line in lines if line.startswith("UNB+"))
        unz_count = sum(1 for line in lines if line.startswith("UNZ+"))
        
        if unh_count != 1 or unt_count != 1 or unb_count != 1 or unz_count != 1:
            logger.error(f"Segment count mismatch: UNH={unh_count}, UNT={unt_count}, UNB={unb_count}, UNZ={unz_count}")
            return False
        
        return True

    def _validate_file_path(self, filename: str) -> None:
        if not filename:
            return
        
        if '/' in filename or '\\' in filename:
            directory = os.path.dirname(filename)
            if directory:
                os.makedirs(directory, exist_ok=True)
        
        if not filename.lower().endswith(('.edi', '.edifact')):
            logger.warning("Recommended file extension is .edi or .edifact")

    def save_to_file(self, filename: Optional[str] = None) -> str:
        message = self.generate()
        if not filename:
            filename = f"invoice_{self.data['invoice_number']}.edi"
        
        self._validate_file_path(filename)
        
        try:
            with open(filename, "w", encoding="utf-8", newline="") as f:
                f.write(message)
            logger.info(f"EDIFACT INVOIC saved to {os.path.abspath(filename)}")
            return filename
        except IOError as e:
            raise EDIFACTGenerationError(f"Failed to write file: {e}", "IO_002")

    @classmethod
    def from_json_file(cls, filepath: str, **kwargs) -> 'EDIFACTGenerator':
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return cls(data, **kwargs)
        except (IOError, json.JSONDecodeError) as e:
            raise EDIFACTGenerationError(f"Failed to load JSON file: {e}", "IO_003")

    def to_dict(self) -> Dict[str, Any]:
        return self.data.copy()

if __name__ == "__main__":
    example_invoice = {
        "invoice_number": "INV12345",
        "invoice_date": "20250509",
        "due_date": "20250609",
        "payment_due_date": "20250609",
        "currency": "EUR",
        "tax_rate": 21.0,
        "payment_terms": "NET30",
        "sender_id": "COMPANY_A",
        "receiver_id": "COMPANY_B",
        "charset": "UNOC",
        "version": "D",
        "application_ref": "INVOICE_APP",
        "ack_request": "1",
        "test_indicator": "0",
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
        config = EDIFACTConfig(DEFAULT_PRECISION=2)
        generator = EDIFACTGenerator(example_invoice, config=config, line_ending="\r\n")
        filepath = generator.save_to_file()
        print(f"EDIFACT file generated: {filepath}")
        
        with open(filepath, 'r') as f:
            print("\nGenerated EDIFACT content:")
            print(f.read())
            
    except EDIFACTBaseError as e:
        logger.error(f"EDIFACT generation failed: {e}")
        if hasattr(e, 'details') and e.details:
            logger.error(f"Error details: {e.details}")
