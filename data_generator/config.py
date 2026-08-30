from pathlib import Path

# -------------------------------
# Data Volume Configuration
# -------------------------------
NUM_CUSTOMERS = 10_000
NUM_MERCHANTS = 500
NUM_BANK_ACCOUNTS = 15_000
NUM_WALLETS = 10_000
NUM_DEVICES = 12_000
NUM_LOCATIONS = 300
NUM_TRANSACTIONS = 100_000
NUM_REFUNDS = 3_000
NUM_SETTLEMENTS = 1_000

# -------------------------------
# Random Seed
# -------------------------------
FAKER_SEED = 42

# -------------------------------
# Output Directory
# -------------------------------
OUTPUT_DIR = str(Path(__file__).resolve().parent / "output")