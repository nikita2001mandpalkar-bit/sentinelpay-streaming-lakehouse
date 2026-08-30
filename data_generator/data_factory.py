"""
Generate realistic business data for SentinelPay.
"""

import random
from datetime import date, timedelta

from faker import Faker

from business_rules import get_salary_range
from logger import get_logger
from master_data import EMAIL_DOMAINS

logger = get_logger(__name__)

fake = Faker("en_IN")


# --------------------------------------------------
# Customer Data
# --------------------------------------------------

def generate_full_name() -> str:
    """
    Generate a random customer full name.

    Returns:
        str: Customer full name.
    """

    return fake.name()


def generate_phone_number() -> str:
    """
    Generate a valid Indian mobile number.

    Returns:
        str: 10-digit mobile number.
    """

    first_digit = random.choice(["6", "7", "8", "9"])
    remaining_digits = "".join(
        random.choices("0123456789", k=9)
    )

    return first_digit + remaining_digits


def generate_email(first_name: str, last_name: str) -> str:
    """
    Generate a realistic email address.

    Args:
        first_name (str): Customer first name.
        last_name (str): Customer last name.

    Returns:
        str: Email address.
    """

    domain = EMAIL_DOMAINS.sample(1).iloc[0]["domain"]

    random_number = random.randint(100, 999)

    return (
        f"{first_name.lower()}."
        f"{last_name.lower()}"
        f"{random_number}"
        f"@{domain}"
    )


def generate_date_of_birth(
    minimum_age: int = 18,
    maximum_age: int = 70,
) -> date:
    """
    Generate a customer's date of birth.

    Args:
        minimum_age (int): Minimum customer age.
        maximum_age (int): Maximum customer age.

    Returns:
        date: Customer date of birth.
    """

    age = random.randint(minimum_age, maximum_age)

    today = date.today()

    return today - timedelta(days=age * 365)


def generate_salary(occupation: str) -> int:
    """
    Generate annual salary based on occupation.

    Args:
        occupation (str): Customer occupation.

    Returns:
        int: Annual salary.
    """

    minimum_salary, maximum_salary = get_salary_range(
        occupation
    )

    return random.randint(
        minimum_salary,
        maximum_salary,
    )