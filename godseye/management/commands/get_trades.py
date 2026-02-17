from django.core.management.base import BaseCommand
from godseye.connectors import master_connection
from godseye.views import normalize_order
from godseye.models import TradeBook
from django.db import close_old_connections
import pandas as pd
from threading import Thread

class Command(BaseCommand):
    help = "Fetch orders for all accounts and store in DB"

    def fetch_and_insert_orders(self, account_key, client):
        """
        Fetch orders for a single account and insert into DB
        """
        close_old_connections()

        # Fetch orders
        if 'jainam' not in account_key.lower():
            try:
                orders = client.get_orders('completed')
            except Exception as e:
                self.stdout.write(f"[{account_key}] Error fetching orders: {e}")
                orders = pd.DataFrame()
        else:
            try:
                orders_dict = client.get_order_book()
                df_orders = pd.DataFrame(orders_dict.get("result", []))
                orders = df_orders[df_orders["OrderStatus"] == "Filled"].reset_index(drop=True)
            except Exception as e:
                self.stdout.write(f"[{account_key}] Error fetching Jainam orders: {e}")
                orders = pd.DataFrame()

        if orders.empty:
            self.stdout.write(f"[{account_key}] No orders found.")
            return

        # # 2️⃣ Fetch existing orderIds ONCE
        existing_ids = set(
            TradeBook.objects.values_list("orderId", flat=True)
        )

        # 3️⃣ Prepare rows
        new_rows = []
        print(account_key)
        for _, order in orders.iterrows():
            normalized = normalize_order(order, account_key.lower())
            if normalized["orderId"] not in existing_ids:
                new_rows.append(TradeBook(**normalized))


        if new_rows:
            TradeBook.objects.bulk_create(new_rows, ignore_conflicts=True)
            self.stdout.write(f"[{account_key}] Inserted {len(new_rows)} orders.")

    def start_thread(self, account_key, client):
        t = Thread(target=self.fetch_and_insert_orders, args=(account_key, client))
        t.start()
        return t

    def handle(self, *args, **options):
        # Load accounts
        masterclass_dict, clients, jainam_user_ids = master_connection('ganesha')

        self.stdout.write(f"Starting order fetch for {len(masterclass_dict)} accounts.")

        threads = []
        for key, client in masterclass_dict.items():
            # Option A: sequential execution (safe)
            # self.fetch_and_insert_orders(key, client)

            # Option B: threaded execution
            t = self.start_thread(key, client)
            threads.append(t)

        # Wait for all threads to finish
        for t in threads:
            t.join()

        self.stdout.write("Order fetch completed for all accounts.")
