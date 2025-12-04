"""
Order Calculation Service

Centralized calculations for all order financial and weight operations.
Used by both single order creation and bulk import.

All calculations are done server-side for security - don't trust client calculations.

Calculation Types:
1. Financial: order_value, tax, total_amount, cod_to_collect
2. Weight: volumetric_weight, applicable_weight
"""

from typing import Dict, List, Any
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP


@dataclass
class OrderCalculations:
    """Result of order calculations"""

    # Financial
    order_value: float
    tax_amount: float
    total_amount: float
    cod_to_collect: float

    # Weight
    volumetric_weight: float
    applicable_weight: float

    def to_dict(self) -> Dict[str, float]:
        """Convert to dictionary for merging with order data"""
        return {
            "order_value": self.order_value,
            "tax_amount": self.tax_amount,
            "total_amount": self.total_amount,
            "cod_to_collect": self.cod_to_collect,
            "volumetric_weight": self.volumetric_weight,
            "applicable_weight": self.applicable_weight,
        }


class OrderCalculationService:
    """
    Centralized calculation service for order operations.

    All monetary values are rounded to 2 decimal places.
    All weight values are rounded to 3 decimal places.

    Usage:
        calculator = OrderCalculationService()
        result = calculator.calculate_order(order_data)
        order_data.update(result.to_dict())
    """

    # Volumetric divisor (standard for couriers)
    VOLUMETRIC_DIVISOR = 5000

    def __init__(self):
        """Initialize calculation service"""
        pass

    # ============================================
    # MAIN CALCULATION METHODS
    # ============================================

    def calculate_order(self, order_data: Dict) -> OrderCalculations:
        """
        Calculate all order values.

        Args:
            order_data: Dictionary containing order fields

        Returns:
            OrderCalculations with all computed values
        """
        # Calculate financial values
        order_value = self.calculate_order_value(order_data.get("products", []))
        tax_amount = self.calculate_tax(order_data, order_value)
        total_amount = self.calculate_total(order_data, order_value, tax_amount)
        cod_to_collect = self.calculate_cod_to_collect(
            order_data.get("payment_mode", "prepaid"),
            total_amount,
            order_data.get("cod_to_collect"),
        )

        # Calculate weights
        volumetric_weight = self.calculate_volumetric_weight(
            order_data.get("length", 0),
            order_data.get("breadth", 0),
            order_data.get("height", 0),
        )
        applicable_weight = self.calculate_applicable_weight(
            order_data.get("weight", 0),
            volumetric_weight,
        )

        return OrderCalculations(
            order_value=order_value,
            tax_amount=tax_amount,
            total_amount=total_amount,
            cod_to_collect=cod_to_collect,
            volumetric_weight=volumetric_weight,
            applicable_weight=applicable_weight,
        )

    # ============================================
    # FINANCIAL CALCULATIONS
    # ============================================

    def calculate_order_value(self, products: List[Dict]) -> float:
        """
        Calculate order value from products.

        Formula: sum(unit_price × quantity) for all products

        Args:
            products: List of product dictionaries

        Returns:
            Order value rounded to 2 decimal places
        """
        if not products:
            return 0.0

        total = Decimal("0")
        for product in products:
            try:
                unit_price = Decimal(str(product.get("unit_price", 0)))
                quantity = Decimal(str(product.get("quantity", 0)))
                total += unit_price * quantity
            except Exception:
                continue

        return float(total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    def calculate_tax(self, order_data: Dict, order_value: float = None) -> float:
        """
        Calculate tax amount.

        Formula: (order_value + charges - discount) × tax_percentage / 100

        Args:
            order_data: Order data dictionary
            order_value: Pre-calculated order value (optional)

        Returns:
            Tax amount rounded to 2 decimal places
        """
        if order_value is None:
            order_value = self.calculate_order_value(order_data.get("products", []))

        # Get tax percentage (default 0)
        tax_percentage = self._to_decimal(order_data.get("tax_percentage", 0))

        if tax_percentage == 0:
            # If no percentage, use provided tax_amount directly
            return self._round_price(order_data.get("tax_amount", 0))

        # Calculate subtotal for tax
        subtotal = Decimal(str(order_value))
        subtotal += self._to_decimal(order_data.get("shipping_charges", 0))
        subtotal += self._to_decimal(order_data.get("cod_charges", 0))
        subtotal += self._to_decimal(order_data.get("gift_wrap_charges", 0))
        subtotal += self._to_decimal(order_data.get("other_charges", 0))
        subtotal -= self._to_decimal(order_data.get("discount", 0))

        # Calculate tax
        tax = subtotal * tax_percentage / Decimal("100")

        return float(tax.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    def calculate_total(
        self,
        order_data: Dict,
        order_value: float = None,
        tax_amount: float = None,
    ) -> float:
        """
        Calculate total order amount.

        Formula: max(0, order_value + all charges - discount + tax)

        FIX: Prevents negative total when discount exceeds order value.

        Args:
            order_data: Order data dictionary
            order_value: Pre-calculated order value (optional)
            tax_amount: Pre-calculated tax amount (optional)

        Returns:
            Total amount rounded to 2 decimal places (minimum 0)
        """
        if order_value is None:
            order_value = self.calculate_order_value(order_data.get("products", []))

        if tax_amount is None:
            tax_amount = self.calculate_tax(order_data, order_value)

        total = Decimal(str(order_value))
        total += self._to_decimal(order_data.get("shipping_charges", 0))
        total += self._to_decimal(order_data.get("cod_charges", 0))
        total += self._to_decimal(order_data.get("gift_wrap_charges", 0))
        total += self._to_decimal(order_data.get("other_charges", 0))
        total -= self._to_decimal(order_data.get("discount", 0))
        total += Decimal(str(tax_amount))

        # FIX: Prevent negative total
        total = max(Decimal("0"), total)

        return float(total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    def calculate_cod_to_collect(
        self,
        payment_mode: str,
        total_amount: float,
        cod_to_collect: float = None,
    ) -> float:
        """
        Calculate COD amount to collect.

        For COD orders:
        - If cod_to_collect provided → use that (partial COD)
        - Otherwise → use total_amount (full COD)

        For Prepaid orders: → 0

        FIX: Now validates that COD amount doesn't exceed total amount.
        If custom COD exceeds total, it's capped at total amount.

        Args:
            payment_mode: 'cod' or 'prepaid'
            total_amount: Total order amount
            cod_to_collect: Custom COD amount for partial COD

        Returns:
            COD amount to collect rounded to 2 decimal places
        """
        if not payment_mode or payment_mode.strip().lower() != "cod":
            return 0.0

        if cod_to_collect is not None and cod_to_collect > 0:
            # Partial COD - use custom amount
            cod_amount = self._round_price(cod_to_collect)
            # FIX: Cap COD at total amount to prevent overcharging
            if cod_amount > total_amount:
                cod_amount = self._round_price(total_amount)
            return cod_amount

        # Full COD - collect total amount
        return self._round_price(total_amount)

    # ============================================
    # WEIGHT CALCULATIONS
    # ============================================

    def calculate_volumetric_weight(
        self,
        length: float,
        breadth: float,
        height: float,
    ) -> float:
        """
        Calculate volumetric weight.

        Formula: (L × B × H) / 5000

        Args:
            length: Length in cm
            breadth: Breadth in cm
            height: Height in cm

        Returns:
            Volumetric weight in kg, rounded to 3 decimal places
        """
        try:
            l = Decimal(str(length or 0))
            b = Decimal(str(breadth or 0))
            h = Decimal(str(height or 0))

            volume = l * b * h
            volumetric = volume / Decimal(str(self.VOLUMETRIC_DIVISOR))

            return float(volumetric.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP))
        except Exception:
            return 0.0

    def calculate_applicable_weight(
        self,
        dead_weight: float,
        volumetric_weight: float = None,
        length: float = None,
        breadth: float = None,
        height: float = None,
    ) -> float:
        """
        Calculate applicable weight (chargeable weight).

        Formula: max(dead_weight, volumetric_weight)

        Args:
            dead_weight: Actual weight in kg
            volumetric_weight: Pre-calculated volumetric weight (optional)
            length/breadth/height: Dimensions if volumetric not provided

        Returns:
            Applicable weight in kg, rounded to 3 decimal places
        """
        if volumetric_weight is None:
            if length is not None and breadth is not None and height is not None:
                volumetric_weight = self.calculate_volumetric_weight(
                    length, breadth, height
                )
            else:
                volumetric_weight = 0.0

        try:
            dead = Decimal(str(dead_weight or 0))
            vol = Decimal(str(volumetric_weight or 0))

            applicable = max(dead, vol)

            return float(applicable.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP))
        except Exception:
            return float(dead_weight or 0)

    # ============================================
    # UTILITY METHODS
    # ============================================

    def _to_decimal(self, value: Any) -> Decimal:
        """Convert value to Decimal, handling None and invalid values"""
        if value is None:
            return Decimal("0")
        try:
            return Decimal(str(value))
        except Exception:
            return Decimal("0")

    def _round_price(self, value: Any) -> float:
        """Round value to 2 decimal places"""
        try:
            d = Decimal(str(value or 0))
            return float(d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        except Exception:
            return 0.0

    def _round_weight(self, value: Any) -> float:
        """Round value to 3 decimal places"""
        try:
            d = Decimal(str(value or 0))
            return float(d.quantize(Decimal("0.001"), rounding=ROUND_HALF_UP))
        except Exception:
            return 0.0

    # ============================================
    # BATCH CALCULATIONS (for bulk import)
    # ============================================

    def calculate_batch(self, orders: List[Dict]) -> List[OrderCalculations]:
        """
        Calculate values for multiple orders.

        Args:
            orders: List of order data dictionaries

        Returns:
            List of OrderCalculations
        """
        return [self.calculate_order(order) for order in orders]
