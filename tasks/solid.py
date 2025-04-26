# SOLID Principles Recap:
#
# S - Single Responsibility Principle
# A class should have only one responsibility.
#
# O - Open/Closed Principle
# A class should be open for extension but closed for modification.
#
# L - Liskov Substitution Principle
# Subtypes must be substitutable for their base types without altering the functionality.
#
# I - Interface Segregation Principle
# Classes should not be forced to implement interfaces they don’t use.
#
# D - Dependency Inversion Principle
# High-level modules should not depend on low-level modules. Both should depend on abstractions.

from abc import ABC, abstractmethod


# --- Single Responsibility Principle ---
# A class to represent an order. It only manages order data.
class Order:
    def __init__(self, items, total_price):
        self.items = items
        self.total_price = total_price

    def display(self):
        print(f"Items: {self.items}, Total Price: ${self.total_price:.2f}")


# --- Open/Closed Principle ---
# Base class for payment processing.
class PaymentProcessor(ABC):
    @abstractmethod
    def process_payment(self, order):
        pass


class CreditCardPaymentProcessor(PaymentProcessor):
    def process_payment(self, order):
        print(f"Processing credit card payment for ${order.total_price:.2f}")


class PayPalPaymentProcessor(PaymentProcessor):
    def process_payment(self, order):
        print(f"Processing PayPal payment for ${order.total_price:.2f}")


class BitcoinPaymentProcessor(PaymentProcessor):
    def process_payment(self, order):
        print(f"Processing Bitcoin payment for ${order.total_price:.2f}")


# --- Liskov Substitution Principle ---
# All `PaymentProcessor` subclasses can be used interchangeably.
def process_order_payment(order, payment_processor):
    if not isinstance(payment_processor, PaymentProcessor):
        raise TypeError("Invalid payment processor")

    payment_processor.process_payment(order)


# --- Interface Segregation Principle ---
# Payment processors should not be forced to implement unnecessary methods.
# Abstract methods are segregated appropriately in `PaymentProcessor`.


# --- Dependency Inversion Principle ---
# High-level modules (e.g., `process_order_payment`) depend on abstractions (`PaymentProcessor`)
# instead of concrete classes.

# Create an order
order = Order(items=["Book", "Laptop"], total_price=1200.50)
order.display()

# Process payments using different processors
credit_card_processor = CreditCardPaymentProcessor()
paypal_processor = PayPalPaymentProcessor()
bitcoin_processor = BitcoinPaymentProcessor()

# Use high-level module to process payment
process_order_payment(order, credit_card_processor)  # Credit card payment
process_order_payment(order, paypal_processor)  # PayPal payment
process_order_payment(order, bitcoin_processor)  # Bitcoin payment


# Explanation of SOLID Principles in the Example

# 1. Single Responsibility Principle (SRP):
# The "Order" class only keeps track of order details (items and total price). It does not handle payment processing.

# 2. Open/Closed Principle (OCP):
# The "PaymentProcessor" base class allows extension by creating new payment types (e.g., "CreditCardPaymentProcessor",
# "PayPalPaymentProcessor", "BitcoinPaymentProcessor").
# You can add new payment processors without modifying existing classes.

# 3. Liskov Substitution Principle (LSP):
# The "process_order_payment" function uses the "PaymentProcessor" abstraction. Any subclass of "PaymentProcessor"
# (e.g., "CreditCardPaymentProcessor", "PayPalPaymentProcessor", "BitcoinPaymentProcessor") can replace each other
# without altering the behavior of the system.

# 4. Interface Segregation Principle (ISP):
# The "PaymentProcessor" interface (abstract base class) defines only the "process_payment" method, ensuring
# that classes implementing it are not forced to define irrelevant methods.

# 5. Dependency Inversion Principle (DIP):
# The "process_order_payment" function depends on the abstract "PaymentProcessor" class instead of any specific payment
# processor implementation. This ensures flexible coupling between high-level modules and low-level modules.
