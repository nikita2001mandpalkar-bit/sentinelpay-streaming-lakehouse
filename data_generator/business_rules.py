"""
Business rules used across SentinelPay.
"""

from logger import get_logger

from master_data import OCCUPATIONS

logger = get_logger(__name__)


# --------------------------------------------------
# Customer Rules
# --------------------------------------------------

def get_salary_range(occupation: str) -> tuple:
    """
    Return the minimum and maximum annual salary
    for a given occupation.
    """

    occupation_data = OCCUPATIONS[
        OCCUPATIONS["occupation"] == occupation
    ]

    if occupation_data.empty:
        logger.error(f"Invalid occupation: {occupation}")
        raise ValueError(f"Invalid occupation: {occupation}")

    minimum_salary = occupation_data.iloc[0]["min_annual_income"]
    maximum_salary = occupation_data.iloc[0]["max_annual_income"]

    return minimum_salary, maximum_salary


def get_credit_risk(occupation: str) -> str:
    """
    Return the credit risk for a given occupation.
    """

    occupation_data = OCCUPATIONS[
        OCCUPATIONS["occupation"] == occupation
    ]

    if occupation_data.empty:
        logger.error(f"Invalid occupation: {occupation}")
        raise ValueError(f"Invalid occupation: {occupation}")

    return occupation_data.iloc[0]["credit_risk"]


def get_average_monthly_transactions(occupation: str) -> int:
    """
    Return the expected monthly transaction count
    for a given occupation.
    """

    occupation_data = OCCUPATIONS[
        OCCUPATIONS["occupation"] == occupation
    ]

    if occupation_data.empty:
        logger.error(f"Invalid occupation: {occupation}")
        raise ValueError(f"Invalid occupation: {occupation}")

    return occupation_data.iloc[0]["average_monthly_transactions"]


def get_average_transaction_amount(occupation: str) -> float:
    """
    Return the expected average transaction amount
    for a given occupation.
    """

    occupation_data = OCCUPATIONS[
        OCCUPATIONS["occupation"] == occupation
    ]

    if occupation_data.empty:
        logger.error(f"Invalid occupation: {occupation}")
        raise ValueError(f"Invalid occupation: {occupation}")

    return occupation_data.iloc[0]["average_transaction_amount"]