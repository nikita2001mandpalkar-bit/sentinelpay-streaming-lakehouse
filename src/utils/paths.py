"""
Centralized storage paths for SentinelPay lakehouse layers.
"""

BRONZE_BASE_PATH = "s3a://sentinelpay-lake/bronze"
SILVER_BASE_PATH = "s3a://sentinelpay-lake/silver"
GOLD_BASE_PATH = "s3a://sentinelpay-lake/gold"
QUARANTINE_BASE_PATH = "s3a://sentinelpay-lake/quarantine"
QUALITY_RESULTS_BASE_PATH = "s3a://sentinelpay-lake/quality_results"
CHECKPOINT_BASE_PATH = "s3a://sentinelpay-lake/checkpoints"
AI_RCA_REPORTS_PATH = "s3a://sentinelpay-lake/ai_rca_reports"


BRONZE_PATHS = {
    "master.customer": f"{BRONZE_BASE_PATH}/master_customer",
    "master.merchant": f"{BRONZE_BASE_PATH}/master_merchant",
    "master.bank_account": f"{BRONZE_BASE_PATH}/master_bank_account",
    "master.wallet": f"{BRONZE_BASE_PATH}/master_wallet",
    "master.device": f"{BRONZE_BASE_PATH}/master_device",
    "event.payment": f"{BRONZE_BASE_PATH}/event_payment",
    "event.refund": f"{BRONZE_BASE_PATH}/event_refund",
    "batch.settlement": f"{BRONZE_BASE_PATH}/batch_settlement",
    "event.wallet": f"{BRONZE_BASE_PATH}/event_wallet",
    "event.merchant": f"{BRONZE_BASE_PATH}/event_merchant",
    "event.payment_json": f"{BRONZE_BASE_PATH}/event_payment_json",
    "event.refund_json": f"{BRONZE_BASE_PATH}/event_refund_json",
    "log.application": f"{BRONZE_BASE_PATH}/log_application",
    "log.error": f"{BRONZE_BASE_PATH}/log_error",
    "log.audit": f"{BRONZE_BASE_PATH}/log_audit",
    "log.support_ticket": f"{BRONZE_BASE_PATH}/log_support_ticket",
    "event.payment.scale": f"{BRONZE_BASE_PATH}/event_payment_scale",
}


CHECKPOINT_PATHS = {
    "master.customer": f"{CHECKPOINT_BASE_PATH}/bronze_master_customer",
    "master.merchant": f"{CHECKPOINT_BASE_PATH}/bronze_master_merchant",
    "master.bank_account": f"{CHECKPOINT_BASE_PATH}/bronze_master_bank_account",
    "master.wallet": f"{CHECKPOINT_BASE_PATH}/bronze_master_wallet",
    "master.device": f"{CHECKPOINT_BASE_PATH}/bronze_master_device",
    "event.payment": f"{CHECKPOINT_BASE_PATH}/bronze_event_payment",
    "event.refund": f"{CHECKPOINT_BASE_PATH}/bronze_event_refund",
    "batch.settlement": f"{CHECKPOINT_BASE_PATH}/bronze_batch_settlement",
    "event.wallet": f"{CHECKPOINT_BASE_PATH}/bronze_event_wallet",
    "event.merchant": f"{CHECKPOINT_BASE_PATH}/bronze_event_merchant",
    "event.payment_json": f"{CHECKPOINT_BASE_PATH}/bronze_event_payment_json",
    "event.refund_json": f"{CHECKPOINT_BASE_PATH}/bronze_event_refund_json",
    "log.application": f"{CHECKPOINT_BASE_PATH}/bronze_log_application",
    "log.error": f"{CHECKPOINT_BASE_PATH}/bronze_log_error",
    "log.audit": f"{CHECKPOINT_BASE_PATH}/bronze_log_audit",
    "log.support_ticket": f"{CHECKPOINT_BASE_PATH}/bronze_log_support_ticket",
    "event.payment.scale": f"{CHECKPOINT_BASE_PATH}/bronze_event_payment_scale",
}


SILVER_PATHS = {
    "event.payment": f"{SILVER_BASE_PATH}/event_payment",
    "event.refund": f"{SILVER_BASE_PATH}/event_refund",
    "log.support_ticket": f"{SILVER_BASE_PATH}/log_support_ticket",
    "log.application": f"{SILVER_BASE_PATH}/log_application",
    "log.error": f"{SILVER_BASE_PATH}/log_error",
    "log.audit": f"{SILVER_BASE_PATH}/log_audit",
    "event.payment.scale": f"{SILVER_BASE_PATH}/event_payment_scale",
}

QUARANTINE_PATHS = {
    "event.payment": f"{QUARANTINE_BASE_PATH}/event_payment",
    "event.payment.scale": f"{QUARANTINE_BASE_PATH}/event_payment_scale",
}

SILVER_CHECKPOINT_PATHS = {
    "event.payment": f"{CHECKPOINT_BASE_PATH}/silver_event_payment",
    "event.refund": f"{CHECKPOINT_BASE_PATH}/silver_event_refund",
    "log.support_ticket": f"{CHECKPOINT_BASE_PATH}/silver_log_support_ticket",
    "log.application": f"{CHECKPOINT_BASE_PATH}/silver_log_application",
    "log.error": f"{CHECKPOINT_BASE_PATH}/silver_log_error",
    "log.audit": f"{CHECKPOINT_BASE_PATH}/silver_log_audit",
    "event.payment.scale": f"{CHECKPOINT_BASE_PATH}/silver_event_payment_scale",
}

GOLD_PATHS = {
    "payment.summary": f"{GOLD_BASE_PATH}/payment_summary",
    "refund.summary": f"{GOLD_BASE_PATH}/refund_summary",
    "support_ticket.summary": f"{GOLD_BASE_PATH}/support_ticket_summary",
}


