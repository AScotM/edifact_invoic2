import json
import logging
import re
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional
from datetime import datetime
import uuid
from jsonschema import validate, ValidationError
import os

# ----------------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------
# Exceptions
# ----------------------------------------------------------------------------
class EDIFACTBaseError(Exception):
    """Base class for EDIFACT errors."""


class EDIFACTValidationError(EDIFACTBaseError):
    """Raised when JSON or field validation fails."""


class EDIFACTGenerationError(EDIFACTBaseError):
    """Raised when segment generation or ordering fails."""


# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------
class EDIFACTConfig:
    SUPPORTED_CHARSETS = {"UNOA", "UNOB", "UTF-8"}
    SUPPORTED_PAYMENT_METHODS = {"31", "42", "ZZZ"}
    SUPPORTED_CURRENCIES = {"EUR", "USD", "GBP"}
    MAX_PARTY_ID_LENGTH = 35
    MAX_NAME_LENGTH = 70
    MAX_ITEM_ID_LENGTH = 35
    MAX_TEXT_LENGTH = 350


# ----------------------------------------------------------------------------
# Validator
# ----------------------------------------------------------------------------
class EDIFACTValidator:
    """Validation of input JSON against schema and EDIFACT rules."""

    JSON_SCHEMA = {
        "type": "object",
        "properties": {
            "charset": {"type": "string"},
            "encoding": {"type": "string"},
            "invoice_number": {"type": "string"},
            "invoice_date": {"type": "string"},
            "currency": {"type": "string"},
            "parties": {
                "type": "object",
                "properties": {
                    "buyer": {"type": "object"},
                    "seller": {"type": "object"},
                },
                "required": ["buyer", "seller"],
            },
            "items": {"type": "array"},
            "payment_terms": {"type": "object"},
        },
        "required": ["invoice_number", "invoice_date", "currency", "parties", "items"],
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
        if data.get("currency") not in EDIFACTConfig.SUPPORTED_CURRENCIES:
            raise EDIFACTValidationError(f"Unsupported currency: {data['currency']}")
        for party in ("buyer", "seller"):
            cls._validate_party(data["parties"][party], party)
        for item in data["items"]:
            cls._validate_item(item)

    @classmethod
    def _validate_party(cls, party: Dict[str, Any], role: str) -> None:
        if not re.match(r"^[A-Z0-9]+$", party.get("id", "")):
            raise EDIFACTValidationError(f"{role} ID must be alphanumeric")
        if len(party["id"]) > EDIFACTConfig.MAX_PARTY_ID_LENGTH:
            raise EDIFACTValidationError(f"{role} ID too long")

    @classmethod
    def _validate_item(cls, item: Dict[str, Any]) -> None:
        if not item.get("id"):
            raise EDIFACTValidationError("Item missing ID")
        if len(item["id"]) > EDIFACTConfig.MAX_ITEM_ID_LENGTH:
            raise EDIFACTValidationError("Item ID too long")


# ----------------------------------------------------------------------------
# Generator
# ----------------------------------------------------------------------------
class EDIFACTGenerator:
    """Generate EDIFACT INVOIC from validated JSON and save to file."""

    def __init__(self, data: Dict[str, Any], precision: int = 2, line_ending: str = "\n"):
        self.data = data
        self.precision = precision
        self.line_ending = line_ending
        self.message_ref = data.get("message_ref") or str(uuid.uuid4().int)[:10]
        self.segments: List[str] = []

    def _format_decimal(self, value: Any) -> str:
        """Format decimal values consistently with precision and trailing zeros."""
        d = Decimal(value).quantize(
            Decimal(f"1.{'0'*self.precision}"), rounding=ROUND_HALF_UP
        )
        return f"{d:.{self.precision}f}"

    def _escape_segment_value(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).replace("?", "??").replace("+", "?+").replace(":", "?:").replace("'", "?'")

    def _build_segment(self, tag: str, elements: List[Any]) -> str:
        parts = [tag] + [self._escape_segment_value(e) for e in elements]
        return "+".join(parts) + "'"

    def generate(self) -> str:
        logger.info("Validating JSON input")
        EDIFACTValidator.validate_schema(self.data)
        EDIFACTValidator.validate_fields(self.data)

        logger.info("Generating EDIFACT segments")

        # Mandatory headers
        self.segments.append("UNH+{}+INVOIC:D:96A:UN'".format(self.message_ref))
        self.segments.append(
            self._build_segment("BGM", ["380", self.data["invoice_number"], "9"])
        )
        self.segments.append(
            self._build_segment("DTM", ["137", self.data["invoice_date"], "102"])
        )

        # Parties
        for role, code in {"buyer": "BY", "seller": "SU"}.items():
            party = self.data["parties"][role]
            self.segments.append(
                self._build_segment("NAD", [code, party["id"], "", "", party.get("name", "")])
            )

        # Line items
        for idx, item in enumerate(self.data["items"], start=1):
            self.segments.append(self._build_segment("LIN", [idx, "", item["id"]]))
            self.segments.append(
                self._build_segment("QTY", ["47", self._format_decimal(item["quantity"])])
            )
            self.segments.append(
                self._build_segment("PRI", ["AAA", self._format_decimal(item["price"])])
            )

        # Summary
        total = sum(Decimal(str(it["quantity"])) * Decimal(str(it["price"])) for it in self.data["items"])
        self.segments.append(self._build_segment("MOA", ["9", self._format_decimal(total)]))
        self.segments.append("UNT+{}+{}'".format(len(self.segments) + 1, self.message_ref))

        return self.line_ending.join(self.segments)

    def save_to_file(self, filename: Optional[str] = None) -> str:
        """Save EDIFACT message to a .edi file and return path."""
        message = self.generate()
        if not filename:
            filename = f"invoice_{self.data['invoice_number']}.edi"
        with open(filename, "w", encoding="utf-8", newline="") as f:
            f.write(message)
        logger.info(f"EDIFACT INVOIC saved to {os.path.abspath(filename)}")
        return filename


# ----------------------------------------------------------------------------
# Example usage
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    example_invoice = {
        "charset": "UNOA",
        "encoding": "UTF-8",
        "invoice_number": "INV12345",
        "invoice_date": datetime.now().strftime("%Y%m%d"),
        "currency": "EUR",
        "parties": {
            "buyer": {"id": "BUYER123", "name": "Buyer Corp"},
            "seller": {"id": "SELLER456", "name": "Seller Ltd"},
        },
        "items": [
            {"id": "ITEM1", "quantity": "10", "price": "5.25"},
            {"id": "ITEM2", "quantity": "3", "price": "12.40"},
        ],
    }

    try:
        generator = EDIFACTGenerator(example_invoice, line_ending="\r\n")  # CRLF if required
        filepath = generator.save_to_file()
        print(f"EDIFACT file generated: {filepath}")
    except EDIFACTBaseError as e:
        logger.error(f"EDIFACT generation failed: {e}")
