"""
Main entry point for SentinelPay Data Generator.
"""

from logger import get_logger

from generators.customer_generator import generate_customers
from generators.merchant_generator import generate_merchants
from generators.bank_account_generator import generate_bank_accounts
from generators.wallet_generator import generate_wallets
from generators.device_generator import generate_devices
from generators.transaction_generator import generate_transactions
from generators.refund_generator import generate_refunds
from generators.settlement_generator import generate_settlements
from generators.json_generator import (
    generate_payment_events,
    generate_wallet_events,
    generate_refund_events,
    generate_merchant_events,
)
from generators.log_generator import (
    generate_application_log,
    generate_error_log,
    generate_audit_log,
)
from generators.support_ticket_generator import (
    generate_support_tickets,
)

logger = get_logger(__name__)


def main() -> None:
    """
    Run full SentinelPay data generation pipeline.
    """

    logger.info("=" * 60)
    logger.info(
        "Starting SentinelPay data generation pipeline..."
    )
    logger.info("=" * 60)

    try:

        customers_df = generate_customers()

        merchants_df = generate_merchants()

        bank_accounts_df = generate_bank_accounts(
            customers_df
        )

        wallets_df = generate_wallets(
            customers_df
        )

        devices_df = generate_devices(
            customers_df
        )

        transactions_df = generate_transactions(
            wallets_df,
            merchants_df,
        )

        refunds_df = generate_refunds(
            transactions_df
        )

        settlements_df = generate_settlements(
            merchants_df,
            transactions_df,
        )

        generate_payment_events(
            transactions_df
        )

        generate_wallet_events(
            wallets_df
        )

        generate_refund_events(
            refunds_df
        )

        generate_merchant_events(
            merchants_df
        )

        generate_application_log(
            transactions_df
        )

        generate_error_log(
            transactions_df
        )

        generate_audit_log(
            customers_df
        )

        generate_support_tickets(
            transactions_df,
            refunds_df,
            wallets_df,
        )

        logger.info("=" * 60)
        logger.info(
            "SentinelPay data generation completed successfully."
        )
        logger.info(
            f"Customers: {len(customers_df):,}"
        )
        logger.info(
            f"Merchants: {len(merchants_df):,}"
        )
        logger.info(
            f"Bank Accounts: {len(bank_accounts_df):,}"
        )
        logger.info(
            f"Wallets: {len(wallets_df):,}"
        )
        logger.info(
            f"Devices: {len(devices_df):,}"
        )
        logger.info(
            f"Transactions: {len(transactions_df):,}"
        )
        logger.info(
            f"Refunds: {len(refunds_df):,}"
        )
        logger.info(
            f"Settlements: {len(settlements_df):,}"
        )
        logger.info("=" * 60)

    except Exception as error:

        logger.exception(
            "SentinelPay data generation pipeline failed."
        )

        raise error


if __name__ == "__main__":
    main()