import math
import os
import time
import logging

from dotenv import load_dotenv
from eth_account import Account
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils.constants import MAINNET_API_URL

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("arb")

SECRET_KEY = os.environ["HL_SECRET_KEY"]
ACCOUNT_ADDRESS = os.environ.get("HL_ACCOUNT_ADDRESS")
THRESHOLD_BPS = float(os.environ.get("ARB_THRESHOLD_BPS", "10"))
SIZE_USD = float(os.environ.get("ARB_SIZE_USD", "1000"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "1.0"))
SLIPPAGE = float(os.environ.get("SLIPPAGE", "0.001"))  # 0.1%
COOLDOWN = float(os.environ.get("COOLDOWN", "30"))  # seconds after a trade

PERP_COIN = os.environ.get("PERP_COIN", "AAVE")
SPOT_COIN = os.environ.get("SPOT_COIN", "AAVE/USDC")

IOC = {"limit": {"tif": "Ioc"}}


def build_clients():
    wallet = Account.from_key(SECRET_KEY)
    info = Info(MAINNET_API_URL, skip_ws=True)
    exchange = Exchange(
        wallet,
        MAINNET_API_URL,
        account_address=ACCOUNT_ADDRESS,
    )
    return info, exchange


def get_sz_decimals(info: Info):
    """Look up szDecimals for the perp and spot coins."""
    meta = info.meta()
    perp_sz_dec = None
    for asset in meta["universe"]:
        if asset["name"] == PERP_COIN:
            perp_sz_dec = asset["szDecimals"]
            break

    spot_asset = info.coin_to_asset.get(info.name_to_coin.get(SPOT_COIN, ""), None)
    spot_sz_dec = info.asset_to_sz_decimals.get(spot_asset, perp_sz_dec)

    log.info("szDecimals: perp=%s spot=%s", perp_sz_dec, spot_sz_dec)
    return perp_sz_dec, spot_sz_dec


def get_spot_book(info: Info):
    book = info.l2_snapshot(SPOT_COIN)
    best_bid = float(book["levels"][0][0]["px"])
    best_ask = float(book["levels"][1][0]["px"])
    return best_bid, best_ask


def get_prices(info: Info):
    mids = info.all_mids()
    perp_mid = float(mids[PERP_COIN])
    spot_bid, spot_ask = get_spot_book(info)
    spot_mid = (spot_bid + spot_ask) / 2
    return spot_mid, spot_bid, spot_ask, perp_mid


def spread_bps(spot_mid: float, perp_mid: float) -> float:
    return (perp_mid - spot_mid) / spot_mid * 10_000


def execute_arb(exchange: Exchange, direction: str, size: float, spot_bid: float, spot_ask: float, perp_mid: float):
    """Place IOC orders on both legs.

    direction: "cash_carry" = buy spot + short perp
                "reverse"    = sell spot + long perp
    """
    if direction == "cash_carry":
        spot_buy = True
        perp_buy = False
        # Cross the spread: buy at ask + slippage, sell at bid - slippage
        spot_px = round(spot_ask * (1 + SLIPPAGE), 1)
        perp_px = round(perp_mid * (1 - SLIPPAGE), 1)
    else:
        spot_buy = False
        perp_buy = True
        spot_px = round(spot_bid * (1 - SLIPPAGE), 1)
        perp_px = round(perp_mid * (1 + SLIPPAGE), 1)

    log.info(
        "Executing %s: spot %s %.5f @ %.1f, perp %s %.5f @ %.1f",
        direction,
        "BUY" if spot_buy else "SELL",
        size,
        spot_px,
        "BUY" if perp_buy else "SELL",
        size,
        perp_px,
    )

    # Execute spot first, only hedge with perp if spot filled
    spot_result = exchange.order(SPOT_COIN, spot_buy, size, spot_px, IOC)
    log.info("Spot result: %s", spot_result)

    spot_status = spot_result["response"]["data"]["statuses"][0]
    if "filled" not in spot_status:
        log.warning("Spot leg failed, skipping perp leg: %s", spot_status)
        return spot_result, None

    filled_sz = float(spot_status["filled"]["totalSz"])
    perp_result = exchange.order(PERP_COIN, perp_buy, filled_sz, perp_px, IOC)
    log.info("Perp result: %s", perp_result)
    return spot_result, perp_result


def main():
    log.info(
        "Starting %s spot-perp arb | threshold=%.1f bps | size=$%.0f | poll=%.1fs",
        PERP_COIN,
        THRESHOLD_BPS,
        SIZE_USD,
        POLL_INTERVAL,
    )

    info, exchange = build_clients()
    perp_sz_dec, spot_sz_dec = get_sz_decimals(info)
    sz_dec = min(perp_sz_dec, spot_sz_dec)

    while True:
        try:
            spot_mid, spot_bid, spot_ask, perp_mid = get_prices(info)
            spread = spread_bps(spot_mid, perp_mid)

            # Ensure size meets $10 minimum notional
            min_size = math.ceil(10 / spot_mid * (10 ** sz_dec)) / (10 ** sz_dec)
            size = max(round(SIZE_USD / spot_mid, sz_dec), min_size)

            log.info(
                "spot=%.1f  perp=%.1f  spread=%+.2f bps",
                spot_mid,
                perp_mid,
                spread,
            )

            if spread > THRESHOLD_BPS:
                execute_arb(exchange, "cash_carry", size, spot_bid, spot_ask, perp_mid)
                time.sleep(COOLDOWN)
            elif spread < -THRESHOLD_BPS:
                execute_arb(exchange, "reverse", size, spot_bid, spot_ask, perp_mid)
                time.sleep(COOLDOWN)

        except Exception:
            log.exception("Error in arb loop")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
