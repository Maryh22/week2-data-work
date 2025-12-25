paid = orders.loc[orders["status_clean"].eq("paid"), ["user_id","amount"]]
print("n_paid:", len(paid))