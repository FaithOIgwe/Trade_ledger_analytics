"""
Fake brokerage activity for the source database.

Two modes:
  seed  - backfill N days of history (run once after `make up`)
  tick  - add a few minutes of new activity on top of what's there
          (Airflow calls this every run, and it's what you watch flow through CDC)

The data is deliberately a bit dirty, because that's the point of the
dbt layer:
  - duplicate execution reports (same exec_id, different fill_id)
  - fills that arrive days after they executed
  - reversed executions
  - orders that sit open and get filled or cancelled on a later run

Usage:
  python generator/generate.py seed --accounts 500 --days 120
  python generator/generate.py tick
"""

import argparse
import os
import random
import uuid
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

import psycopg

SYMBOLS = {
    "AAPL": 190, "MSFT": 410, "NVDA": 120, "AMZN": 180, "GOOGL": 165, "META": 480,
    "TSLA": 220, "JPM": 200, "V": 270, "KO": 62, "SPY": 520, "QQQ": 440,
}
FIRST = ["Ada", "Tunde", "Chioma", "Emeka", "Zainab", "Kofi", "Amara", "Musa", "Ifeoma",
         "Lerato", "Sipho", "Wanjiru", "Yusuf", "Nneka", "Kwame", "Ngozi", "David", "Sara",
         "Liam", "Maria", "Chen", "Priya", "Omar", "Fatima", "Jonas", "Elena"]
LAST = ["Okafor", "Adeyemi", "Mensah", "Nwosu", "Bello", "Diallo", "Kamau", "Okonkwo",
        "Abubakar", "Eze", "Mokoena", "Asante", "Smith", "Garcia", "Wang", "Patel",
        "Hassan", "Schmidt", "Rossi", "Silva"]
COUNTRIES = ["NG", "NG", "NG", "GH", "KE", "ZA", "US", "GB", "DE", "BR", "IN"]

# rates for the messy bits
P_REJECT = 0.04
P_CANCEL = 0.06
P_LIMIT_STAYS_OPEN = 0.35
P_DUPLICATE_REPORT = 0.03
P_LATE_ARRIVAL = 0.02
P_REVERSAL = 0.006

CENT = Decimal("0.01")
FOUR = Decimal("0.0001")


def money(x) -> Decimal:
    return Decimal(str(x)).quantize(CENT, rounding=ROUND_HALF_UP)


def px(x) -> Decimal:
    return Decimal(str(x)).quantize(FOUR, rounding=ROUND_HALF_UP)


def dsn() -> str:
    return os.getenv(
        "SOURCE_DATABASE_URL",
        "postgresql://brokerage:brokerage@localhost:5432/brokerage",
    )


def weekdays(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def market_time(d: date) -> datetime:
    # US session is roughly 14:30-21:00 UTC
    seconds = random.randint(14 * 3600 + 1800, 21 * 3600)
    return datetime.combine(d, time(0), tzinfo=timezone.utc) + timedelta(seconds=seconds)


class Simulator:
    def __init__(self, conn):
        self.conn = conn
        self.accounts = {}                      # account_id -> dict
        self.positions = defaultdict(Decimal)   # (account_id, symbol) -> qty
        self.cash = defaultdict(Decimal)        # account_id -> balance
        self.prices = {}                        # (symbol, date) -> close

        self.new_accounts, self.new_orders, self.new_fills, self.new_cash = [], [], [], []
        self.order_updates = []                 # (status, updated_at, order_id)
        self.account_updates = []               # (status, tier, updated_at, account_id)

    # ---------- prices ----------

    def load_prices(self, start: date, end: date):
        with self.conn.cursor() as cur:
            cur.execute(
                "select symbol, bar_date, close from raw.market_prices "
                "where bar_date between %s and %s",
                (start, end),
            )
            for sym, d, close in cur.fetchall():
                self.prices[(sym, d)] = Decimal(close)

    def fill_missing_prices(self, start: date, end: date):
        """Random-walk closes for any (symbol, weekday) we don't have.
        Real Alpaca bars win; these only fill gaps so the generator never stalls."""
        rows = []
        for sym, base in SYMBOLS.items():
            last = Decimal(base)
            for d in weekdays(start, end):
                if (sym, d) in self.prices:
                    last = self.prices[(sym, d)]
                    continue
                last = px(float(last) * (1 + random.gauss(0.0004, 0.018)))
                self.prices[(sym, d)] = last
                rows.append((sym, d, last, "synthetic"))
        if rows:
            with self.conn.cursor() as cur:
                cur.executemany(
                    "insert into raw.market_prices (symbol, bar_date, close, source) "
                    "values (%s, %s, %s, %s) on conflict (symbol, bar_date) do nothing",
                    rows,
                )

    def price_on(self, sym: str, d: date) -> Decimal:
        probe = d
        for _ in range(10):
            if (sym, probe) in self.prices:
                return self.prices[(sym, probe)]
            probe -= timedelta(days=1)
        return Decimal(SYMBOLS[sym])

    # ---------- accounts ----------

    def create_account(self, created_at: datetime):
        acct_id = uuid.uuid4()
        acct = {
            "account_id": acct_id,
            "customer_name": f"{random.choice(FIRST)} {random.choice(LAST)}",
            "country": random.choice(COUNTRIES),
            "account_type": random.choices(["cash", "margin"], [0.8, 0.2])[0],
            "status": "active",
            "tier": random.choices(["basic", "plus", "pro"], [0.7, 0.22, 0.08])[0],
            "created_at": created_at,
            "updated_at": created_at,
        }
        self.accounts[acct_id] = acct
        self.new_accounts.append(acct)
        deposit = money(random.choice([500, 1000, 2500, 5000, 10000, 25000]))
        self.add_cash(acct_id, "deposit", deposit, None, created_at + timedelta(minutes=5))

    def maybe_change_account(self, acct_id, when: datetime):
        acct = self.accounts[acct_id]
        if random.random() < 0.6:
            order = ["basic", "plus", "pro"]
            idx = order.index(acct["tier"])
            acct["tier"] = order[min(idx + 1, 2)] if random.random() < 0.75 else order[max(idx - 1, 0)]
        else:
            acct["status"] = random.choice(["restricted", "active", "closed"])
        acct["updated_at"] = when
        self.account_updates.append((acct["status"], acct["tier"], when, acct_id))

    def random_deposits(self, when: datetime, k: int):
        active = [a["account_id"] for a in self.accounts.values()
                  if a["status"] == "active" and a["created_at"] < when]
        for acct_id in random.sample(active, k=min(k, len(active))):
            amt = money(random.choice([200, 500, 1000, 2000, 5000]))
            self.add_cash(acct_id, "deposit", amt, None, when - timedelta(minutes=random.randint(5, 90)))

    # ---------- cash ----------

    def add_cash(self, acct_id, kind, amount: Decimal, ref, when):
        self.cash[acct_id] += amount
        self.new_cash.append((uuid.uuid4(), acct_id, kind, amount, "USD", ref, when))

    # ---------- orders and fills ----------

    def new_order(self, when: datetime, allow_open: bool):
        active = [a for a in self.accounts.values() if a["status"] == "active" and a["created_at"] < when]
        if not active:
            return
        acct = random.choice(active)
        acct_id = acct["account_id"]
        sym = random.choice(list(SYMBOLS))
        ref_price = self.price_on(sym, when.date())

        held = self.positions[(acct_id, sym)]
        side = "sell" if held > 0 and random.random() < 0.4 else "buy"
        if side == "sell":
            qty = px(max(1, int(held * Decimal(random.choice([0.25, 0.5, 1])))))
            qty = min(qty, held)
        else:
            budget = self.cash[acct_id] * Decimal(random.uniform(0.05, 0.3))
            if acct["account_type"] == "margin":
                budget *= 2
            qty = px(int(budget / ref_price))
            if qty < 1:
                return

        order_type = "limit" if random.random() < 0.45 else "market"
        limit_price = px(float(ref_price) * random.uniform(0.98, 1.01)) if order_type == "limit" else None

        order = {
            "order_id": uuid.uuid4(),
            "client_order_id": f"co-{uuid.uuid4().hex[:12]}",
            "account_id": acct_id, "symbol": sym, "side": side,
            "order_type": order_type, "qty": qty, "limit_price": limit_price,
            "status": "new", "submitted_at": when, "updated_at": when,
        }

        roll = random.random()
        if roll < P_REJECT:
            order["status"] = "rejected"
            order["updated_at"] = when + timedelta(milliseconds=300)
        elif order_type == "limit" and allow_open and random.random() < P_LIMIT_STAYS_OPEN:
            pass  # sits open; a later tick fills or cancels it
        else:
            self.execute(order, ref_price, when, cancel_after_partial=roll < P_REJECT + P_CANCEL)

        self.new_orders.append(order)

    def execute(self, order, ref_price: Decimal, when: datetime, cancel_after_partial=False):
        """Fill an order in 1-3 pieces. Mutates order status in place."""
        remaining = order["qty"]
        pieces = random.choice([1, 1, 2, 3])
        t = when
        filled = Decimal(0)
        for i in range(pieces):
            if remaining <= 0:
                break
            if cancel_after_partial and i == 1:
                break
            q = remaining if i == pieces - 1 else px(max(1, int(remaining / 2)))
            q = min(q, remaining)
            price = order["limit_price"] or px(float(ref_price) * random.uniform(0.998, 1.002))
            t = t + timedelta(seconds=random.randint(1, 90))
            self.record_fill(order, q, price, t)
            remaining -= q
            filled += q

        if cancel_after_partial:
            order["status"] = "canceled"
        elif remaining > 0:
            order["status"] = "partially_filled"
        else:
            order["status"] = "filled"
        order["updated_at"] = t

    def record_fill(self, order, qty: Decimal, price: Decimal, executed_at: datetime):
        exec_id = f"EX{uuid.uuid4().hex[:16].upper()}"
        ingested = executed_at + timedelta(seconds=random.randint(1, 20))
        if random.random() < P_LATE_ARRIVAL:
            ingested = executed_at + timedelta(days=random.randint(1, 3), minutes=random.randint(0, 600))
        # a report can't arrive in the future (this broke the incremental model once)
        ingested = max(executed_at, min(ingested, datetime.now(timezone.utc)))

        self.new_fills.append((uuid.uuid4(), exec_id, order["order_id"], qty, price,
                               executed_at, ingested, False, None))
        if random.random() < P_DUPLICATE_REPORT:
            self.new_fills.append((uuid.uuid4(), exec_id, order["order_id"], qty, price,
                                   executed_at, ingested + timedelta(seconds=random.randint(5, 600)),
                                   False, None))

        acct_id = order["account_id"]
        sign = -1 if order["side"] == "buy" else 1
        self.positions[(acct_id, order["symbol"])] += qty * -sign
        self.add_cash(acct_id, "trade_settlement", money(sign * qty * price), exec_id, executed_at)
        fee = money(max(Decimal("0.35"), qty * Decimal("0.005")))
        self.add_cash(acct_id, "fee", -fee, exec_id, executed_at)

        # occasionally an execution gets busted later. Only reverse buys so the
        # position can't go negative.
        if order["side"] == "buy" and random.random() < P_REVERSAL:
            rev_at = executed_at + timedelta(hours=random.randint(1, 30))
            rev_at = max(executed_at + timedelta(seconds=1), min(rev_at, datetime.now(timezone.utc)))
            rev_exec = f"EX{uuid.uuid4().hex[:16].upper()}"
            self.new_fills.append((uuid.uuid4(), rev_exec, order["order_id"], qty, price,
                                   rev_at, rev_at + timedelta(seconds=3), True, exec_id))
            self.positions[(acct_id, order["symbol"])] -= qty
            self.add_cash(acct_id, "trade_settlement", money(qty * price), rev_exec, rev_at)

    def work_open_orders(self, when: datetime, limit=40):
        with self.conn.cursor() as cur:
            cur.execute(
                "select order_id, client_order_id, account_id, symbol, side, order_type, qty, "
                "limit_price, status, submitted_at from brokerage.orders "
                "where status = 'new' order by submitted_at limit %s",
                (limit,),
            )
            cols = [c.name for c in cur.description]
            open_orders = [dict(zip(cols, r)) for r in cur.fetchall()]

        for o in open_orders:
            roll = random.random()
            if roll < 0.5:
                continue
            if roll < 0.8:
                if o["side"] == "sell" and self.positions[(o["account_id"], o["symbol"])] < o["qty"]:
                    o["status"] = "canceled"
                    o["updated_at"] = when
                else:
                    self.execute(o, self.price_on(o["symbol"], when.date()), when)
            else:
                o["status"] = "canceled"
                o["updated_at"] = when
            self.order_updates.append((o["status"], o["updated_at"], o["order_id"]))

    # ---------- state from the db (tick mode) ----------

    def load_state(self):
        with self.conn.cursor() as cur:
            cur.execute("select account_id, customer_name, country, account_type, status, tier, "
                        "created_at, updated_at from brokerage.accounts")
            cols = [c.name for c in cur.description]
            for r in cur.fetchall():
                a = dict(zip(cols, r))
                self.accounts[a["account_id"]] = a
            cur.execute("""
                select o.account_id, o.symbol,
                       sum(case when o.side = 'buy' then 1 else -1 end
                           * case when f.is_reversal then -1 else 1 end * f.qty)
                from (select distinct on (exec_id) * from brokerage.fills order by exec_id, ingested_at) f
                join brokerage.orders o using (order_id)
                group by 1, 2
            """)
            for acct, sym, qty in cur.fetchall():
                self.positions[(acct, sym)] = Decimal(qty)
            cur.execute("select account_id, sum(amount) from brokerage.cash_movements group by 1")
            for acct, bal in cur.fetchall():
                self.cash[acct] = Decimal(bal)

    # ---------- write ----------

    def flush(self):
        with self.conn.cursor() as cur:
            cur.executemany(
                "insert into brokerage.accounts values (%(account_id)s, %(customer_name)s, %(country)s, "
                "%(account_type)s, %(status)s, %(tier)s, %(created_at)s, %(updated_at)s)",
                self.new_accounts,
            )
            cur.executemany(
                "insert into brokerage.orders values (%(order_id)s, %(client_order_id)s, %(account_id)s, "
                "%(symbol)s, %(side)s, %(order_type)s, %(qty)s, %(limit_price)s, %(status)s, "
                "%(submitted_at)s, %(updated_at)s)",
                self.new_orders,
            )
            cur.executemany("insert into brokerage.fills values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            self.new_fills)
            cur.executemany("insert into brokerage.cash_movements values (%s, %s, %s, %s, %s, %s, %s)",
                            self.new_cash)
            cur.executemany("update brokerage.orders set status = %s, updated_at = %s where order_id = %s",
                            self.order_updates)
            cur.executemany("update brokerage.accounts set status = %s, tier = %s, updated_at = %s "
                            "where account_id = %s", self.account_updates)
        self.conn.commit()
        print(f"accounts +{len(self.new_accounts)} (~{len(self.account_updates)} changed) | "
              f"orders +{len(self.new_orders)} (~{len(self.order_updates)} updated) | "
              f"fills +{len(self.new_fills)} | cash rows +{len(self.new_cash)}")


def seed(args):
    # history stops at yesterday; today's activity comes from `tick`, so
    # nothing in the seed can be timestamped after "now"
    end = datetime.now(timezone.utc).date() - timedelta(days=1)
    start = end - timedelta(days=args.days)
    with psycopg.connect(dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute("select count(*) from brokerage.orders")
            if cur.fetchone()[0] and not args.force:
                raise SystemExit("brokerage.orders already has data. Use --force to add more anyway.")

        sim = Simulator(conn)
        sim.load_prices(start - timedelta(days=10), end)
        sim.fill_missing_prices(start - timedelta(days=10), end)

        # most accounts exist from the start, the rest sign up along the way
        days = list(weekdays(start, end))
        for i in range(args.accounts):
            d = days[0] if i < args.accounts * 0.6 else random.choice(days)
            sim.create_account(market_time(d) - timedelta(hours=8))

        for d in days:
            sim.random_deposits(market_time(d), k=max(1, args.accounts // 25))
            n = max(1, int(random.gauss(args.orders_per_day, args.orders_per_day * 0.2)))
            for _ in range(n):
                sim.new_order(market_time(d), allow_open=(d == end))
            # a handful of tier/status changes per day
            for acct_id in random.sample(list(sim.accounts), k=min(2, len(sim.accounts))):
                if random.random() < 0.3:
                    sim.maybe_change_account(acct_id, market_time(d))

        # in seed mode the account changes are already applied to the dicts,
        # so the insert carries the final state; drop the update list
        sim.account_updates = []
        sim.flush()


def tick(args):
    now = datetime.now(timezone.utc)
    with psycopg.connect(dsn()) as conn:
        sim = Simulator(conn)
        sim.load_state()
        if not sim.accounts:
            raise SystemExit("No accounts yet. Run `seed` first.")
        sim.load_prices(now.date() - timedelta(days=10), now.date())
        sim.fill_missing_prices(now.date() - timedelta(days=10), now.date())

        if random.random() < 0.3:
            sim.create_account(now - timedelta(minutes=random.randint(1, 30)))
        sim.random_deposits(now, k=5)
        # keep everything (including the 1-3 fill pieces after each order) before "now"
        sim.work_open_orders(now - timedelta(minutes=5))
        for _ in range(args.orders):
            sim.new_order(now - timedelta(seconds=random.randint(300, 900)), allow_open=True)
        for acct_id in random.sample(list(sim.accounts), k=min(3, len(sim.accounts))):
            if random.random() < 0.5:
                sim.maybe_change_account(acct_id, now)
        sim.flush()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--random-seed", type=int, default=None)
    sub = p.add_subparsers(dest="mode", required=True)

    s = sub.add_parser("seed")
    s.add_argument("--accounts", type=int, default=500)
    s.add_argument("--days", type=int, default=120)
    s.add_argument("--orders-per-day", type=int, default=400)
    s.add_argument("--force", action="store_true")

    t = sub.add_parser("tick")
    t.add_argument("--orders", type=int, default=60)

    args = p.parse_args()
    if args.random_seed is not None:
        random.seed(args.random_seed)
    seed(args) if args.mode == "seed" else tick(args)


if __name__ == "__main__":
    main()
