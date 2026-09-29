import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, NotRequired, Optional, Sequence, TypedDict, Union


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


CONTROL_CHAR_REGEX = re.compile(r"[\x00-\x1F\x7F]")


DATE_FORMATS = {
    "102": "%Y%m%d",
    "203": "%Y%m%d%H%M",
    "101": "%y%m%d",
}


SEGMENT_CODES = {
    "INVOICE_TYPE": "380",
    "CREDIT_NOTE_TYPE": "381",
    "DEBIT_NOTE_TYPE": "383",
    "DATE_ISSUED": "137",
    "DATE_DUE": "13",
    "DATE_PAYMENT_DUE": "12",
    "CURRENCY_INVOICE": "2",
    "PARTY_BUYER": "BY",
    "PARTY_SELLER": "SE",
    "LOCATION_PLACE": "11",
    "COMMUNICATION_TELEPHONE": "TE",
    "COMMUNICATION_EMAIL": "EM",
    "ITEM_IDENTIFICATION": "EN",
    "QUALIFIER_ORDERED": "47",
    "PRICE_NET": "AAA",
    "TAX_SERVICE": "7",
    "TAX_VAT": "VAT",
    "MOA_LINE_TOTAL": "79",
    "MOA_TAX_TOTAL": "124",
    "MOA_INVOICE_TOTAL": "86",
    "FTX_TEXT": "AAI",
    "FII_ACCOUNT": "BE",
}


class PartyDict(TypedDict):
    id: str
    name: NotRequired[str]
    address: NotRequired[str]
    contact: NotRequired[str]


class BankAccountDict(TypedDict):
    account: str
    bank_code: NotRequired[str]


class ItemDict(TypedDict):
    id: str
    quantity: Decimal
    price: Decimal
    description: NotRequired[str]
    unit: NotRequired[str]
    tax_category: NotRequired[str]
    tax_rate: NotRequired[Decimal]


class InvoiceDict(TypedDict):
    invoice_number: str
    invoice_date: str
    currency: str
    parties: Dict[str, PartyDict]
    items: List[ItemDict]
    due_date: NotRequired[str]
    payment_due_date: NotRequired[str]
    tax_rate: NotRequired[Decimal]
    payment_terms: NotRequired[str]
    sender_id: NotRequired[str]
    receiver_id: NotRequired[str]
    charset: NotRequired[str]
    version: NotRequired[str]
    application_ref: NotRequired[str]
    ack_request: NotRequired[str]
    test_indicator: NotRequired[str]
    notes: NotRequired[str]
    bank_account: NotRequired[BankAccountDict]
    message_ref: NotRequired[str]
    interchange_ref: NotRequired[str]
    agreement_id: NotRequired[str]
    priority: NotRequired[str]


class EDIFACTBaseError(Exception):
    pass


class EDIFACTValidationError(EDIFACTBaseError):
    def __init__(
        self,
        message: str,
        code: str = "VALID_001",
        details: Optional[Dict[str, Any]] = None,
    ):
        self.code = code
        self.details = details or {}
        super().__init__(f"{code}: {message}")


class EDIFACTGenerationError(EDIFACTBaseError):
    def __init__(
        self,
        message: str,
        code: str = "GEN_001",
        details: Optional[Dict[str, Any]] = None,
    ):
        self.code = code
        self.details = details or {}
        super().__init__(f"{code}: {message}")


class EDIFACTConfig:
    SUPPORTED_CHARSETS = {"UNOA", "UNOB", "UNOC"}
    SUPPORTED_CURRENCIES = {"EUR", "USD", "GBP", "JPY", "CAD"}
    SUPPORTED_DATE_FORMATS = {"102", "203", "101"}
    SUPPORTED_PAYMENT_TERMS = {
        "NET15",
        "NET30",
        "NET45",
        "NET60",
        "CASH",
    }

    MAX_PARTY_ID_LENGTH = 35
    MAX_NAME_LENGTH = 70
    MAX_ITEM_ID_LENGTH = 35
    MAX_TEXT_LENGTH = 350
    MAX_DECIMAL_PLACES = 6
    MAX_SEGMENT_LENGTH = 2000
    MAX_FILE_SIZE_MB = 10
    MAX_RETRIES = 3

    SEGMENT_TERMINATOR = "'"
    DATA_ELEMENT_SEPARATOR = "+"
    COMPONENT_SEPARATOR = ":"
    REPETITION_SEPARATOR = "*"
    DECIMAL_NOTATION = "."
    RELEASE_CHARACTER = "?"

    DEFAULT_PRECISION = 2
    DEFAULT_VERSION = "D"
    DEFAULT_RELEASE = "96A"

    def __init__(self, **kwargs: Any):
        for key, value in kwargs.items():
            if not hasattr(self, key):
                raise EDIFACTValidationError(
                    f"Unknown configuration option: {key}",
                    "CONFIG_001",
                    {"option": key},
                )
            setattr(self, key, value)

        self._validate()

    def _validate(self) -> None:
        separators = [
            self.SEGMENT_TERMINATOR,
            self.DATA_ELEMENT_SEPARATOR,
            self.COMPONENT_SEPARATOR,
            self.REPETITION_SEPARATOR,
            self.DECIMAL_NOTATION,
            self.RELEASE_CHARACTER,
        ]

        if any(len(value) != 1 for value in separators):
            raise EDIFACTValidationError(
                "EDIFACT separators must be single characters",
                "CONFIG_002",
            )

        if len(set(separators)) != len(separators):
            raise EDIFACTValidationError(
                "EDIFACT separators must be unique",
                "CONFIG_003",
            )

        if self.DEFAULT_PRECISION < 0:
            raise EDIFACTValidationError(
                "DEFAULT_PRECISION cannot be negative",
                "CONFIG_004",
            )

        if self.DEFAULT_PRECISION > self.MAX_DECIMAL_PLACES:
            raise EDIFACTValidationError(
                "DEFAULT_PRECISION cannot exceed MAX_DECIMAL_PLACES",
                "CONFIG_005",
            )

        if self.MAX_FILE_SIZE_MB <= 0:
            raise EDIFACTValidationError(
                "MAX_FILE_SIZE_MB must be positive",
                "CONFIG_006",
            )

        if self.MAX_RETRIES < 1:
            raise EDIFACTValidationError(
                "MAX_RETRIES must be at least 1",
                "CONFIG_007",
            )


@dataclass(frozen=True)
class Composite:
    components: Sequence[Any]


Element = Union[Any, Composite]


class EDIFACTValidator:
    @classmethod
    def validate(cls, data: InvoiceDict, config: EDIFACTConfig) -> None:
        cls.validate_schema(data, config)
        cls.validate_fields(data, config)
        cls.validate_interdependencies(data)

    @classmethod
    def validate_schema(
        cls,
        data: InvoiceDict,
        config: EDIFACTConfig,
    ) -> None:
        if not isinstance(data, dict):
            raise EDIFACTValidationError(
                "Invoice must be an object",
                "SCHEMA_001",
            )

        required_fields = (
            "invoice_number",
            "invoice_date",
            "currency",
            "parties",
            "items",
        )

        for field in required_fields:
            if field not in data:
                raise EDIFACTValidationError(
                    f"Missing required field: {field}",
                    "SCHEMA_002",
                    {"missing_field": field},
                )

        cls._require_nonempty_string(
            "invoice_number",
            data["invoice_number"],
            35,
        )

        cls._require_nonempty_string(
            "invoice_date",
            data["invoice_date"],
            14,
        )

        cls._require_nonempty_string(
            "currency",
            data["currency"],
            3,
        )

        if len(data["currency"]) != 3:
            raise EDIFACTValidationError(
                "Currency code must contain exactly 3 characters",
                "SCHEMA_003",
                {"currency": data["currency"]},
            )

        if not isinstance(data["parties"], dict):
            raise EDIFACTValidationError(
                "Parties must be an object",
                "SCHEMA_004",
            )

        for role in ("buyer", "seller"):
            if role not in data["parties"]:
                raise EDIFACTValidationError(
                    f"Missing required party: {role}",
                    "SCHEMA_005",
                    {"party": role},
                )

            party = data["parties"][role]

            if not isinstance(party, dict):
                raise EDIFACTValidationError(
                    f"{role} must be an object",
                    "SCHEMA_006",
                    {"party": role},
                )

            if "id" not in party:
                raise EDIFACTValidationError(
                    f"{role} ID is required",
                    "SCHEMA_007",
                    {"party": role},
                )

            cls._require_nonempty_string(
                f"{role}.id",
                party["id"],
                config.MAX_PARTY_ID_LENGTH,
            )

            if "name" in party:
                cls._validate_optional_string(
                    f"{role}.name",
                    party["name"],
                    config.MAX_NAME_LENGTH,
                )

            if "address" in party:
                cls._validate_optional_string(
                    f"{role}.address",
                    party["address"],
                    config.MAX_TEXT_LENGTH,
                )

            if "contact" in party:
                cls._validate_optional_string(
                    f"{role}.contact",
                    party["contact"],
                    config.MAX_TEXT_LENGTH,
                )

        if not isinstance(data["items"], list):
            raise EDIFACTValidationError(
                "Items must be a list",
                "SCHEMA_008",
            )

        if not data["items"]:
            raise EDIFACTValidationError(
                "At least one item is required",
                "SCHEMA_009",
            )

        for index, item in enumerate(data["items"]):
            if not isinstance(item, dict):
                raise EDIFACTValidationError(
                    f"Item {index} must be an object",
                    "SCHEMA_010",
                    {"item_index": index},
                )

            for field in ("id", "quantity", "price"):
                if field not in item:
                    raise EDIFACTValidationError(
                        f"Item {index} is missing {field}",
                        "SCHEMA_011",
                        {
                            "item_index": index,
                            "missing_field": field,
                        },
                    )

            cls._require_nonempty_string(
                f"items[{index}].id",
                item["id"],
                config.MAX_ITEM_ID_LENGTH,
            )

            if "description" in item:
                cls._validate_optional_string(
                    f"items[{index}].description",
                    item["description"],
                    config.MAX_TEXT_LENGTH,
                )

            if "unit" in item:
                cls._validate_optional_string(
                    f"items[{index}].unit",
                    item["unit"],
                    3,
                )

            if "tax_category" in item:
                cls._validate_optional_string(
                    f"items[{index}].tax_category",
                    item["tax_category"],
                    3,
                )

        if data.get("notes") is not None:
            cls._validate_optional_string(
                "notes",
                data["notes"],
                config.MAX_TEXT_LENGTH,
            )

        if data.get("bank_account") is not None:
            bank = data["bank_account"]

            if not isinstance(bank, dict):
                raise EDIFACTValidationError(
                    "bank_account must be an object",
                    "SCHEMA_012",
                )

            if not bank.get("account"):
                raise EDIFACTValidationError(
                    "Bank account number is required",
                    "SCHEMA_013",
                )

    @classmethod
    def validate_fields(
        cls,
        data: InvoiceDict,
        config: EDIFACTConfig,
    ) -> None:
        charset = data.get("charset", "UNOC")

        if charset not in config.SUPPORTED_CHARSETS:
            raise EDIFACTValidationError(
                f"Unsupported charset: {charset}",
                "VALID_002",
                {"supported": sorted(config.SUPPORTED_CHARSETS)},
            )

        currency = data["currency"].upper()

        if currency not in config.SUPPORTED_CURRENCIES:
            raise EDIFACTValidationError(
                f"Unsupported currency: {currency}",
                "VALID_003",
                {"supported": sorted(config.SUPPORTED_CURRENCIES)},
            )

        cls._validate_date(
            data["invoice_date"],
            "invoice_date",
        )

        if data.get("due_date"):
            cls._validate_date(
                data["due_date"],
                "due_date",
            )

        if data.get("payment_due_date"):
            cls._validate_date(
                data["payment_due_date"],
                "payment_due_date",
            )

        if data.get("payment_terms"):
            if data["payment_terms"] not in config.SUPPORTED_PAYMENT_TERMS:
                raise EDIFACTValidationError(
                    f"Unsupported payment terms: {data['payment_terms']}",
                    "VALID_014",
                    {
                        "supported_terms": sorted(
                            config.SUPPORTED_PAYMENT_TERMS
                        )
                    },
                )

        for role in ("buyer", "seller"):
            cls._validate_party(
                data["parties"][role],
                role,
                config,
            )

        for index, item in enumerate(data["items"]):
            cls._validate_item(
                item,
                index,
                config,
            )

        if data.get("tax_rate") is not None:
            cls._validate_tax_rate(
                data["tax_rate"],
                "tax_rate",
            )

        if data.get("ack_request") is not None:
            ack = str(data["ack_request"])

            if ack not in {"0", "1"}:
                raise EDIFACTValidationError(
                    "ack_request must be 0 or 1",
                    "VALID_016",
                )

        if data.get("test_indicator") is not None:
            indicator = str(data["test_indicator"])

            if indicator not in {"0", "1"}:
                raise EDIFACTValidationError(
                    "test_indicator must be 0 or 1",
                    "VALID_017",
                )

    @classmethod
    def validate_interdependencies(
        cls,
        data: InvoiceDict,
    ) -> None:
        invoice_date = datetime.strptime(
            data["invoice_date"],
            DATE_FORMATS["102"],
        )

        if data.get("due_date"):
            due_date = datetime.strptime(
                data["due_date"],
                DATE_FORMATS["102"],
            )

            if due_date <= invoice_date:
                raise EDIFACTValidationError(
                    "Due date must be after invoice date",
                    "VALID_012",
                )

        if data.get("payment_due_date"):
            payment_due_date = datetime.strptime(
                data["payment_due_date"],
                DATE_FORMATS["102"],
            )

            if payment_due_date < invoice_date:
                raise EDIFACTValidationError(
                    "Payment due date cannot be before invoice date",
                    "VALID_015",
                )

            if data.get("due_date"):
                due_date = datetime.strptime(
                    data["due_date"],
                    DATE_FORMATS["102"],
                )

                if payment_due_date < due_date:
                    raise EDIFACTValidationError(
                        "Payment due date cannot be before due date",
                        "VALID_018",
                    )

        item_ids = [item["id"] for item in data["items"]]

        if len(item_ids) != len(set(item_ids)):
            raise EDIFACTValidationError(
                "Item IDs must be unique",
                "VALID_013",
            )

    @classmethod
    def _validate_party(
        cls,
        party: PartyDict,
        role: str,
        config: EDIFACTConfig,
    ) -> None:
        party_id = party.get("id", "")

        if not party_id:
            raise EDIFACTValidationError(
                f"{role} ID is required",
                "VALID_006",
            )

        if len(party_id) > config.MAX_PARTY_ID_LENGTH:
            raise EDIFACTValidationError(
                f"{role} ID too long",
                "VALID_007",
                {
                    "role": role,
                    "length": len(party_id),
                    "maximum": config.MAX_PARTY_ID_LENGTH,
                },
            )

    @classmethod
    def _validate_item(
        cls,
        item: ItemDict,
        index: int,
        config: EDIFACTConfig,
    ) -> None:
        if len(item["id"]) > config.MAX_ITEM_ID_LENGTH:
            raise EDIFACTValidationError(
                f"Item {index} ID too long",
                "VALID_009",
            )

        quantity = cls._decimal_value(
            item["quantity"],
            f"items[{index}].quantity",
        )

        price = cls._decimal_value(
            item["price"],
            f"items[{index}].price",
        )

        if quantity <= 0:
            raise EDIFACTValidationError(
                f"Item {index} quantity must be positive",
                "VALID_010",
                {"quantity": str(quantity)},
            )

        if price < 0:
            raise EDIFACTValidationError(
                f"Item {index} price must be non-negative",
                "VALID_011",
                {"price": str(price)},
            )

        cls._validate_decimal_places(
            quantity,
            config.MAX_DECIMAL_PLACES,
            f"items[{index}].quantity",
        )

        cls._validate_decimal_places(
            price,
            config.MAX_DECIMAL_PLACES,
            f"items[{index}].price",
        )

        if item.get("tax_rate") is not None:
            cls._validate_tax_rate(
                item["tax_rate"],
                f"items[{index}].tax_rate",
            )

    @classmethod
    def _validate_tax_rate(
        cls,
        value: Any,
        field_name: str,
    ) -> None:
        rate = cls._decimal_value(value, field_name)

        if rate < 0 or rate > 100:
            raise EDIFACTValidationError(
                f"{field_name} must be between 0 and 100",
                "VALID_019",
                {"value": str(rate)},
            )

    @classmethod
    def _validate_date(
        cls,
        date_str: str,
        field_name: str,
        date_format: str = "102",
    ) -> None:
        if date_format not in DATE_FORMATS:
            raise EDIFACTValidationError(
                f"Unsupported date format: {date_format}",
                "VALID_004",
            )

        try:
            datetime.strptime(
                date_str,
                DATE_FORMATS[date_format],
            )
        except (TypeError, ValueError):
            raise EDIFACTValidationError(
                f"Invalid date in {field_name}: {date_str}",
                "VALID_005",
            )

    @classmethod
    def _decimal_value(
        cls,
        value: Any,
        field_name: str,
    ) -> Decimal:
        try:
            result = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            raise EDIFACTValidationError(
                f"Invalid decimal value for {field_name}: {value}",
                "VALID_020",
            )

        if not result.is_finite():
            raise EDIFACTValidationError(
                f"{field_name} must be finite",
                "VALID_021",
            )

        return result

    @classmethod
    def _validate_decimal_places(
        cls,
        value: Decimal,
        maximum: int,
        field_name: str,
    ) -> None:
        exponent = value.as_tuple().exponent
        places = max(0, -exponent)

        if places > maximum:
            raise EDIFACTValidationError(
                f"{field_name} has too many decimal places",
                "VALID_022",
                {
                    "decimal_places": places,
                    "maximum": maximum,
                },
            )

    @classmethod
    def _require_nonempty_string(
        cls,
        field_name: str,
        value: Any,
        max_length: int,
    ) -> None:
        if not isinstance(value, str):
            raise EDIFACTValidationError(
                f"{field_name} must be a string",
                "SCHEMA_014",
            )

        if not value.strip():
            raise EDIFACTValidationError(
                f"{field_name} cannot be empty",
                "SCHEMA_015",
            )

        if len(value) > max_length:
            raise EDIFACTValidationError(
                f"{field_name} exceeds maximum length of {max_length}",
                "SCHEMA_016",
                {
                    "field": field_name,
                    "length": len(value),
                },
            )

    @classmethod
    def _validate_optional_string(
        cls,
        field_name: str,
        value: Any,
        max_length: int,
    ) -> None:
        if not isinstance(value, str):
            raise EDIFACTValidationError(
                f"{field_name} must be a string",
                "SCHEMA_017",
            )

        if len(value) > max_length:
            raise EDIFACTValidationError(
                f"{field_name} exceeds maximum length of {max_length}",
                "SCHEMA_018",
                {
                    "field": field_name,
                    "length": len(value),
                },
            )


class EDIFACTGenerator:
    def __init__(
        self,
        data: InvoiceDict,
        config: Optional[EDIFACTConfig] = None,
        line_ending: str = "\n",
    ):
        self.config = config or EDIFACTConfig()
        self.data = self._sanitize_input(data)
        self.line_ending = line_ending

        self.message_ref = (
            self.data.get("message_ref")
            or self._generate_reference()
        )

        self.interchange_ref = (
            self.data.get("interchange_ref")
            or self._generate_reference()
        )

        self.segments: List[str] = []
        self._generated = False

    @staticmethod
    def _generate_reference() -> str:
        return str(uuid.uuid4().int)[:14]

    def _sanitize_input(self, data: Any) -> Any:
        if isinstance(data, str):
            return CONTROL_CHAR_REGEX.sub("", data)

        if isinstance(data, dict):
            return {
                key: self._sanitize_input(value)
                for key, value in data.items()
            }

        if isinstance(data, list):
            return [
                self._sanitize_input(value)
                for value in data
            ]

        return data

    def _to_decimal(self, value: Any) -> Decimal:
        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise EDIFACTGenerationError(
                f"Invalid numeric value: {value}",
                "GEN_003",
                {"error": str(exc)},
            )

        if not decimal_value.is_finite():
            raise EDIFACTGenerationError(
                f"Numeric value must be finite: {value}",
                "GEN_013",
            )

        return decimal_value

    def _format_decimal(
        self,
        value: Any,
        precision: Optional[int] = None,
    ) -> str:
        decimal_value = self._to_decimal(value)

        exponent = decimal_value.as_tuple().exponent
        decimal_places = max(0, -exponent)

        if decimal_places > self.config.MAX_DECIMAL_PLACES:
            raise EDIFACTGenerationError(
                f"Too many decimal places in {value}",
                "GEN_007",
                {
                    "decimal_places": decimal_places,
                    "max_allowed": self.config.MAX_DECIMAL_PLACES,
                },
            )

        if precision is None:
            precision = self.config.DEFAULT_PRECISION

        quantum = Decimal(1).scaleb(-precision)

        quantized = decimal_value.quantize(
            quantum,
            rounding=ROUND_HALF_UP,
        )

        formatted = f"{quantized:.{precision}f}"

        if self.config.DECIMAL_NOTATION != ".":
            formatted = formatted.replace(
                ".",
                self.config.DECIMAL_NOTATION,
            )

        return formatted

    def _escape_value(self, value: Any) -> str:
        if value is None:
            return ""

        result: List[str] = []

        reserved = {
            self.config.SEGMENT_TERMINATOR,
            self.config.DATA_ELEMENT_SEPARATOR,
            self.config.COMPONENT_SEPARATOR,
            self.config.REPETITION_SEPARATOR,
        }

        for char in str(value):
            if CONTROL_CHAR_REGEX.match(char):
                continue

            if char == self.config.RELEASE_CHARACTER:
                result.append(self.config.RELEASE_CHARACTER)
                result.append(self.config.RELEASE_CHARACTER)
                continue

            if char in reserved:
                result.append(self.config.RELEASE_CHARACTER)
                result.append(char)
                continue

            result.append(char)

        return "".join(result)

    def _serialize_element(self, element: Element) -> str:
        if isinstance(element, Composite):
            return self.config.COMPONENT_SEPARATOR.join(
                self._escape_value(component)
                for component in element.components
            )

        return self._escape_value(element)

    def _build_segment(
        self,
        tag: str,
        elements: Sequence[Element],
    ) -> str:
        if not tag or not tag.isalnum():
            raise EDIFACTGenerationError(
                f"Invalid segment tag: {tag}",
                "GEN_014",
            )

        serialized = [
            self._serialize_element(element)
            for element in elements
        ]

        while serialized and serialized[-1] == "":
            serialized.pop()

        if serialized:
            segment = (
                tag
                + self.config.DATA_ELEMENT_SEPARATOR
                + self.config.DATA_ELEMENT_SEPARATOR.join(serialized)
                + self.config.SEGMENT_TERMINATOR
            )
        else:
            segment = (
                tag
                + self.config.SEGMENT_TERMINATOR
            )

        self._validate_segment_length(segment)
        return segment

    def _validate_segment_length(self, segment: str) -> None:
        if len(segment) > self.config.MAX_SEGMENT_LENGTH:
            raise EDIFACTGenerationError(
                f"Segment too long: {len(segment)}",
                "GEN_004",
                {
                    "length": len(segment),
                    "maximum": self.config.MAX_SEGMENT_LENGTH,
                },
            )

    def _append(
        self,
        tag: str,
        elements: Sequence[Element],
    ) -> None:
        self.segments.append(
            self._build_segment(tag, elements)
        )

    def _add_una_segment(self) -> None:
        segment = (
            "UNA"
            + self.config.COMPONENT_SEPARATOR
            + self.config.DATA_ELEMENT_SEPARATOR
            + self.config.DECIMAL_NOTATION
            + self.config.RELEASE_CHARACTER
            + self.config.REPETITION_SEPARATOR
            + self.config.SEGMENT_TERMINATOR
        )

        self.segments.append(segment)

    def _add_unb_segment(self) -> None:
        timestamp = datetime.now()

        charset = self.data.get(
            "charset",
            "UNOC",
        )

        syntax_version = self.data.get(
            "version",
            self.config.DEFAULT_VERSION,
        )

        sender_id = self.data.get(
            "sender_id",
            "SENDER",
        )

        receiver_id = self.data.get(
            "receiver_id",
            "RECEIVER",
        )

        application_ref = self.data.get(
            "application_ref",
            "",
        )

        priority = self.data.get(
            "priority",
            "",
        )

        ack_request = self.data.get(
            "ack_request",
            "0",
        )

        agreement_id = self.data.get(
            "agreement_id",
            "",
        )

        test_indicator = self.data.get(
            "test_indicator",
            "1",
        )

        elements: List[Element] = [
            Composite(
                [
                    charset,
                    syntax_version,
                ]
            ),
            sender_id,
            receiver_id,
            Composite(
                [
                    timestamp.strftime("%y%m%d"),
                    timestamp.strftime("%H%M"),
                ]
            ),
            self.interchange_ref,
            "",
            application_ref,
            priority,
            ack_request,
            agreement_id,
            test_indicator,
        ]

        self._append(
            "UNB",
            elements,
        )

    def _add_unz_segment(self) -> None:
        message_count = sum(
            1
            for segment in self.segments
            if segment.startswith("UNH+")
        )

        self._append(
            "UNZ",
            [
                str(message_count),
                self.interchange_ref,
            ],
        )

    def _add_header_segments(self) -> None:
        self._append(
            "UNH",
            [
                self.message_ref,
                Composite(
                    [
                        "INVOIC",
                        self.config.DEFAULT_VERSION,
                        self.config.DEFAULT_RELEASE,
                        "UN",
                    ]
                ),
            ],
        )

        self._append(
            "BGM",
            [
                SEGMENT_CODES["INVOICE_TYPE"],
                self.data["invoice_number"],
                "9",
            ],
        )

        self._append(
            "DTM",
            [
                Composite(
                    [
                        SEGMENT_CODES["DATE_ISSUED"],
                        self.data["invoice_date"],
                        "102",
                    ]
                )
            ],
        )

        if self.data.get("due_date"):
            self._append(
                "DTM",
                [
                    Composite(
                        [
                            SEGMENT_CODES["DATE_DUE"],
                            self.data["due_date"],
                            "102",
                        ]
                    )
                ],
            )

        if self.data.get("payment_terms"):
            self._append(
                "PAI",
                [
                    Composite(
                        [
                            self.data["payment_terms"],
                            "3",
                        ]
                    )
                ],
            )

        if self.data.get("payment_due_date"):
            self._append(
                "DTM",
                [
                    Composite(
                        [
                            SEGMENT_CODES["DATE_PAYMENT_DUE"],
                            self.data["payment_due_date"],
                            "102",
                        ]
                    )
                ],
            )

    def _add_currency_segment(self) -> None:
        self._append(
            "CUX",
            [
                Composite(
                    [
                        SEGMENT_CODES["CURRENCY_INVOICE"],
                        self.data["currency"].upper(),
                        "9",
                    ]
                )
            ],
        )

    def _add_party_segments(self) -> None:
        party_mapping = {
            "buyer": SEGMENT_CODES["PARTY_BUYER"],
            "seller": SEGMENT_CODES["PARTY_SELLER"],
        }

        for role, qualifier in party_mapping.items():
            party = self.data["parties"][role]

            self._append(
                "NAD",
                [
                    qualifier,
                    Composite(
                        [
                            party["id"],
                            "",
                            "91",
                        ]
                    ),
                    "",
                    "",
                    party.get("name", ""),
                ],
            )

            if party.get("address"):
                self._append(
                    "LOC",
                    [
                        SEGMENT_CODES["LOCATION_PLACE"],
                        party["address"],
                    ],
                )

            if party.get("contact"):
                communication_type = (
                    SEGMENT_CODES["COMMUNICATION_EMAIL"]
                    if "@" in party["contact"]
                    else SEGMENT_CODES["COMMUNICATION_TELEPHONE"]
                )

                self._append(
                    "COM",
                    [
                        Composite(
                            [
                                party["contact"],
                                communication_type,
                            ]
                        )
                    ],
                )

    def _effective_tax_rate(
        self,
        item: ItemDict,
    ) -> Optional[Decimal]:
        if item.get("tax_rate") is not None:
            return self._to_decimal(
                item["tax_rate"]
            )

        if self.data.get("tax_rate") is not None:
            return self._to_decimal(
                self.data["tax_rate"]
            )

        return None

    def _add_line_items(self) -> None:
        if len(self.data["items"]) > 999999:
            raise EDIFACTGenerationError(
                "Too many line items",
                "GEN_011",
                {"count": len(self.data["items"])},
            )

        for index, item in enumerate(
            self.data["items"],
            start=1,
        ):
            self._append(
                "LIN",
                [
                    str(index),
                    "",
                    Composite(
                        [
                            item["id"],
                            SEGMENT_CODES["ITEM_IDENTIFICATION"],
                        ]
                    ),
                ],
            )

            if item.get("description"):
                self._append(
                    "IMD",
                    [
                        "F",
                        "",
                        "",
                        "",
                        item["description"],
                    ],
                )

            unit = item.get(
                "unit",
                "PCE",
            )

            self._append(
                "QTY",
                [
                    Composite(
                        [
                            SEGMENT_CODES["QUALIFIER_ORDERED"],
                            self._format_decimal(
                                item["quantity"]
                            ),
                            unit,
                        ]
                    )
                ],
            )

            self._append(
                "PRI",
                [
                    Composite(
                        [
                            SEGMENT_CODES["PRICE_NET"],
                            self._format_decimal(
                                item["price"]
                            ),
                        ]
                    )
                ],
            )

            tax_rate = self._effective_tax_rate(item)

            if (
                item.get("tax_category")
                or tax_rate is not None
            ):
                tax_category = item.get(
                    "tax_category",
                    SEGMENT_CODES["TAX_VAT"],
                )

                tax_components: List[Any] = [
                    SEGMENT_CODES["TAX_SERVICE"],
                    tax_category,
                    "",
                    "",
                    "",
                ]

                if tax_rate is not None:
                    tax_components.append(
                        self._format_decimal(
                            tax_rate
                        )
                    )

                self._append(
                    "TAX",
                    [
                        Composite(
                            tax_components
                        )
                    ],
                )

    def _add_ftx_segments(self) -> None:
        notes = self.data.get("notes")

        if not notes:
            return

        chunk_size = 70

        chunks = [
            notes[index:index + chunk_size]
            for index in range(
                0,
                len(notes),
                chunk_size,
            )
        ]

        for chunk in chunks:
            self._append(
                "FTX",
                [
                    SEGMENT_CODES["FTX_TEXT"],
                    "",
                    "",
                    Composite(
                        [
                            chunk,
                        ]
                    ),
                ],
            )

    def _add_payment_instructions(self) -> None:
        bank_data = self.data.get(
            "bank_account"
        )

        if not bank_data:
            return

        account = bank_data.get(
            "account"
        )

        if not account:
            return

        bank_code = bank_data.get(
            "bank_code",
            "",
        )

        self._append(
            "FII",
            [
                SEGMENT_CODES["FII_ACCOUNT"],
                Composite(
                    [
                        account,
                        "",
                    ]
                ),
                Composite(
                    [
                        bank_code,
                    ]
                )
                if bank_code
                else "",
            ],
        )

    def _calculate_totals(
        self,
    ) -> tuple[
        Decimal,
        Dict[Decimal, Decimal],
        Decimal,
        Decimal,
    ]:
        subtotal = Decimal("0")
        tax_bases: Dict[Decimal, Decimal] = {}

        for item in self.data["items"]:
            quantity = self._to_decimal(
                item["quantity"]
            )

            price = self._to_decimal(
                item["price"]
            )

            line_total = quantity * price
            subtotal += line_total

            tax_rate = self._effective_tax_rate(
                item
            )

            if tax_rate is not None:
                tax_bases[tax_rate] = (
                    tax_bases.get(
                        tax_rate,
                        Decimal("0"),
                    )
                    + line_total
                )

        quantum = Decimal(1).scaleb(
            -self.config.DEFAULT_PRECISION
        )

        subtotal = subtotal.quantize(
            quantum,
            rounding=ROUND_HALF_UP,
        )

        tax_amounts: Dict[Decimal, Decimal] = {}

        for rate, base in tax_bases.items():
            amount = (
                base
                * rate
                / Decimal("100")
            ).quantize(
                quantum,
                rounding=ROUND_HALF_UP,
            )

            tax_amounts[rate] = amount

        total_tax = sum(
            tax_amounts.values(),
            Decimal("0"),
        ).quantize(
            quantum,
            rounding=ROUND_HALF_UP,
        )

        invoice_total = (
            subtotal
            + total_tax
        ).quantize(
            quantum,
            rounding=ROUND_HALF_UP,
        )

        return (
            subtotal,
            tax_amounts,
            total_tax,
            invoice_total,
        )

    def _add_summary_segments(self) -> None:
        (
            subtotal,
            tax_amounts,
            total_tax,
            invoice_total,
        ) = self._calculate_totals()

        self._append(
            "MOA",
            [
                Composite(
                    [
                        SEGMENT_CODES["MOA_LINE_TOTAL"],
                        self._format_decimal(
                            subtotal
                        ),
                    ]
                )
            ],
        )

        for rate in sorted(
            tax_amounts.keys()
        ):
            self._append(
                "TAX",
                [
                    Composite(
                        [
                            SEGMENT_CODES["TAX_SERVICE"],
                            SEGMENT_CODES["TAX_VAT"],
                            "",
                            "",
                            "",
                            self._format_decimal(
                                rate
                            ),
                        ]
                    )
                ],
            )

            self._append(
                "MOA",
                [
                    Composite(
                        [
                            SEGMENT_CODES["MOA_TAX_TOTAL"],
                            self._format_decimal(
                                tax_amounts[rate]
                            ),
                        ]
                    )
                ],
            )

        if tax_amounts:
            self._append(
                "MOA",
                [
                    Composite(
                        [
                            SEGMENT_CODES["MOA_TAX_TOTAL"],
                            self._format_decimal(
                                total_tax
                            ),
                        ]
                    )
                ],
            )

        self._append(
            "MOA",
            [
                Composite(
                    [
                        SEGMENT_CODES["MOA_INVOICE_TOTAL"],
                        self._format_decimal(
                            invoice_total
                        ),
                    ]
                )
            ],
        )

    def _add_unt_segment(self) -> None:
        unh_index = self._find_single_segment(
            "UNH"
        )

        segment_count = (
            len(self.segments)
            - unh_index
            + 1
        )

        self._append(
            "UNT",
            [
                str(segment_count),
                self.message_ref,
            ],
        )

    def _find_single_segment(
        self,
        tag: str,
    ) -> int:
        prefix = (
            tag
            + self.config.DATA_ELEMENT_SEPARATOR
        )

        indices = [
            index
            for index, segment
            in enumerate(self.segments)
            if segment.startswith(prefix)
        ]

        if len(indices) != 1:
            raise EDIFACTGenerationError(
                f"Expected exactly one {tag} segment",
                "GEN_005",
                {
                    "tag": tag,
                    "count": len(indices),
                },
            )

        return indices[0]

    def generate(self) -> str:
        if self._generated:
            return self.line_ending.join(
                self.segments
            )

        logger.info(
            "Starting EDIFACT generation for invoice %s",
            self.data.get(
                "invoice_number",
                "Unknown",
            ),
        )

        EDIFACTValidator.validate(
            self.data,
            self.config,
        )

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

        content = self.line_ending.join(
            self.segments
        )

        content_size_mb = (
            len(content.encode("utf-8"))
            / (1024 * 1024)
        )

        if (
            content_size_mb
            > self.config.MAX_FILE_SIZE_MB
        ):
            raise EDIFACTGenerationError(
                (
                    "Generated content too large: "
                    f"{content_size_mb:.2f}MB"
                ),
                "GEN_012",
                {
                    "size_mb": content_size_mb,
                    "maximum_mb": (
                        self.config.MAX_FILE_SIZE_MB
                    ),
                },
            )

        self.validate_edifact_syntax(
            content
        )

        self._generated = True

        logger.info(
            "Generated %d EDIFACT segments",
            len(self.segments),
        )

        return content

    def _parse_segment(
        self,
        segment: str,
    ) -> tuple[str, List[str]]:
        segment = segment.rstrip(
            self.config.SEGMENT_TERMINATOR
        )

        parts = segment.split(
            self.config.DATA_ELEMENT_SEPARATOR
        )

        return parts[0], parts[1:]

    def validate_edifact_syntax(
        self,
        content: str,
    ) -> bool:
        lines = [
            line
            for line in content.split(
                self.line_ending
            )
            if line
        ]

        if not lines:
            raise EDIFACTGenerationError(
                "Generated EDIFACT content is empty",
                "SYNTAX_001",
            )

        if not lines[0].startswith("UNA"):
            raise EDIFACTGenerationError(
                "Missing UNA segment",
                "SYNTAX_002",
            )

        for index, line in enumerate(
            lines[1:],
            start=2,
        ):
            if not line.endswith(
                self.config.SEGMENT_TERMINATOR
            ):
                raise EDIFACTGenerationError(
                    f"Segment {index} has no terminator",
                    "SYNTAX_003",
                )

            if (
                len(line)
                > self.config.MAX_SEGMENT_LENGTH
            ):
                raise EDIFACTGenerationError(
                    f"Segment {index} exceeds maximum length",
                    "SYNTAX_004",
                )

        required_singletons = (
            "UNB",
            "UNH",
            "UNT",
            "UNZ",
        )

        parsed = [
            self._parse_segment(line)
            for line in lines[1:]
        ]

        tags = [
            tag
            for tag, _ in parsed
        ]

        for tag in required_singletons:
            count = tags.count(tag)

            if count != 1:
                raise EDIFACTGenerationError(
                    (
                        f"Expected one {tag} segment, "
                        f"found {count}"
                    ),
                    "SYNTAX_005",
                    {
                        "tag": tag,
                        "count": count,
                    },
                )

        if tags[0] != "UNB":
            raise EDIFACTGenerationError(
                "UNB must follow UNA",
                "SYNTAX_006",
            )

        if tags[1] != "UNH":
            raise EDIFACTGenerationError(
                "UNH must follow UNB",
                "SYNTAX_007",
            )

        if tags[-2] != "UNT":
            raise EDIFACTGenerationError(
                "UNT must precede UNZ",
                "SYNTAX_008",
            )

        if tags[-1] != "UNZ":
            raise EDIFACTGenerationError(
                "UNZ must be the final segment",
                "SYNTAX_009",
            )

        parsed_by_tag: Dict[
            str,
            List[List[str]],
        ] = {}

        for tag, elements in parsed:
            parsed_by_tag.setdefault(
                tag,
                [],
            ).append(elements)

        unh = parsed_by_tag["UNH"][0]
        unt = parsed_by_tag["UNT"][0]
        unb = parsed_by_tag["UNB"][0]
        unz = parsed_by_tag["UNZ"][0]

        if len(unh) < 1 or len(unt) < 2:
            raise EDIFACTGenerationError(
                "Invalid UNH or UNT structure",
                "SYNTAX_010",
            )

        if unh[0] != unt[1]:
            raise EDIFACTGenerationError(
                "UNH and UNT message references do not match",
                "SYNTAX_011",
                {
                    "UNH": unh[0],
                    "UNT": unt[1],
                },
            )

        try:
            declared_message_segments = int(
                unt[0]
            )
        except (TypeError, ValueError):
            raise EDIFACTGenerationError(
                "Invalid UNT segment count",
                "SYNTAX_012",
            )

        unh_position = tags.index("UNH")
        unt_position = tags.index("UNT")

        actual_message_segments = (
            unt_position
            - unh_position
            + 1
        )

        if (
            declared_message_segments
            != actual_message_segments
        ):
            raise EDIFACTGenerationError(
                (
                    "UNT segment count mismatch: "
                    f"declared={declared_message_segments}, "
                    f"actual={actual_message_segments}"
                ),
                "SYNTAX_013",
            )

        if len(unb) < 5 or len(unz) < 2:
            raise EDIFACTGenerationError(
                "Invalid UNB or UNZ structure",
                "SYNTAX_014",
            )

        interchange_reference = unb[4]

        if interchange_reference != unz[1]:
            raise EDIFACTGenerationError(
                "UNB and UNZ interchange references do not match",
                "SYNTAX_015",
                {
                    "UNB": interchange_reference,
                    "UNZ": unz[1],
                },
            )

        try:
            declared_message_count = int(
                unz[0]
            )
        except (TypeError, ValueError):
            raise EDIFACTGenerationError(
                "Invalid UNZ message count",
                "SYNTAX_016",
            )

        actual_message_count = tags.count(
            "UNH"
        )

        if (
            declared_message_count
            != actual_message_count
        ):
            raise EDIFACTGenerationError(
                (
                    "UNZ message count mismatch: "
                    f"declared={declared_message_count}, "
                    f"actual={actual_message_count}"
                ),
                "SYNTAX_017",
            )

        return True

    def _validate_file_path(
        self,
        filename: str,
        create_dirs: bool = False,
    ) -> None:
        if not filename:
            raise EDIFACTGenerationError(
                "Filename cannot be empty",
                "IO_001",
            )

        directory = os.path.dirname(
            os.path.abspath(filename)
        )

        if not os.path.exists(directory):
            if create_dirs:
                try:
                    os.makedirs(
                        directory,
                        exist_ok=True,
                    )
                except OSError as exc:
                    raise EDIFACTGenerationError(
                        f"Unable to create directory: {exc}",
                        "IO_004",
                    )
            else:
                raise EDIFACTGenerationError(
                    f"Directory does not exist: {directory}",
                    "IO_004",
                )

        if not filename.lower().endswith(
            (".edi", ".edifact")
        ):
            logger.warning(
                "Recommended file extension is .edi or .edifact"
            )

    def save_to_file(
        self,
        filename: Optional[str] = None,
        create_dirs: bool = False,
        max_retries: Optional[int] = None,
    ) -> str:
        message = self.generate()

        if filename is None:
            filename = (
                f"invoice_{self.data['invoice_number']}.edi"
            )

        self._validate_file_path(
            filename,
            create_dirs,
        )

        retries = (
            self.config.MAX_RETRIES
            if max_retries is None
            else max_retries
        )

        if retries < 1:
            raise EDIFACTGenerationError(
                "max_retries must be at least 1",
                "IO_005",
            )

        last_error: Optional[OSError] = None

        for attempt in range(
            1,
            retries + 1,
        ):
            try:
                with open(
                    filename,
                    "w",
                    encoding="utf-8",
                    newline="",
                ) as file:
                    file.write(message)

                absolute_path = os.path.abspath(
                    filename
                )

                logger.info(
                    "EDIFACT INVOIC saved to %s",
                    absolute_path,
                )

                return filename

            except OSError as exc:
                last_error = exc

                if attempt < retries:
                    time.sleep(1)

        raise EDIFACTGenerationError(
            (
                f"Failed to write file after "
                f"{retries} attempts: {last_error}"
            ),
            "IO_002",
        )

    @classmethod
    def from_json_file(
        cls,
        filepath: str,
        **kwargs: Any,
    ) -> "EDIFACTGenerator":
        try:
            with open(
                filepath,
                "r",
                encoding="utf-8",
            ) as file:
                data = json.load(file)

        except OSError as exc:
            raise EDIFACTGenerationError(
                f"Failed to read JSON file: {exc}",
                "IO_003",
            )

        except json.JSONDecodeError as exc:
            raise EDIFACTGenerationError(
                f"Invalid JSON file: {exc}",
                "IO_006",
            )

        if not isinstance(data, dict):
            raise EDIFACTGenerationError(
                "JSON root must be an object",
                "IO_007",
            )

        return cls(
            data,
            **kwargs,
        )

    def to_dict(self) -> InvoiceDict:
        return self.data.copy()

    def __enter__(self) -> "EDIFACTGenerator":
        return self

    def __exit__(
        self,
        exc_type: Any,
        exc_val: Any,
        exc_tb: Any,
    ) -> None:
        if (
            not self._generated
            and exc_type is None
        ):
            self.generate()


if __name__ == "__main__":
    example_invoice: InvoiceDict = {
        "invoice_number": "INV12345",
        "invoice_date": "20250509",
        "due_date": "20250609",
        "payment_due_date": "20250609",
        "currency": "EUR",
        "tax_rate": Decimal("21.00"),
        "payment_terms": "NET30",
        "sender_id": "COMPANY_A",
        "receiver_id": "COMPANY_B",
        "charset": "UNOC",
        "version": "4",
        "application_ref": "INVOICE_APP",
        "ack_request": "1",
        "test_indicator": "0",
        "notes": (
            "Thank you for your business. "
            "Please note that payments should be made "
            "within 30 days."
        ),
        "bank_account": {
            "account": "NL91ABNA0417164300",
            "bank_code": "ABNANL2A",
        },
        "parties": {
            "buyer": {
                "id": "BUYER123",
                "name": "Buyer Corporation",
                "address": "123 Main St",
                "contact": "buyer@example.com",
            },
            "seller": {
                "id": "SELLER456",
                "name": "Seller Ltd",
                "address": "456 Oak Ave",
                "contact": "sales@seller.com",
            },
        },
        "items": [
            {
                "id": "ITEM001",
                "description": "Premium Widget",
                "quantity": Decimal("10"),
                "price": Decimal("25.50"),
                "unit": "PCE",
                "tax_category": "VAT",
                "tax_rate": Decimal("21.00"),
            },
            {
                "id": "ITEM002",
                "description": "Standard Widget",
                "quantity": Decimal("5"),
                "price": Decimal("15.75"),
                "unit": "PCE",
                "tax_category": "VAT",
                "tax_rate": Decimal("21.00"),
            },
        ],
    }

    try:
        config = EDIFACTConfig(
            DEFAULT_PRECISION=2
        )

        with EDIFACTGenerator(
            example_invoice,
            config=config,
            line_ending="\r\n",
        ) as generator:
            filepath = generator.save_to_file(
                create_dirs=True
            )

            print(
                f"EDIFACT file generated: {filepath}"
            )

            print()
            print("Generated EDIFACT content:")
            print()

            with open(
                filepath,
                "r",
                encoding="utf-8",
            ) as file:
                print(file.read())

    except EDIFACTBaseError as exc:
        logger.error(
            "EDIFACT generation failed: %s",
            exc,
        )

        details = getattr(
            exc,
            "details",
            None,
        )

        if details:
            logger.error(
                "Error details: %s",
                details,
            )
