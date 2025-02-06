from hopsworks import udf
from datetime import datetime 

@udf([int, str, datetime, float, str, str, int], mode='python')
def fetch_and_create_transactions_features(tid : int, 
                                    context: dict):
    """
    Transformation function that uses storage connector to fetch data from postgres and create on-demand features from the data.
    """
    conn = context["connection"]
    
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM transactions WHERE tid = %s", (tid,))
        tid, data = cur.fetchone()

    data["transaction_time"] = datetime.strptime(data["datetime"], "%Y-%m-%d %H:%M:%S")

    
    # Return on-demand feature as well as other features required
    return data["cc_num"], data["category"], data["transaction_time"], data["amount"], data["city"], data["country"], data["fraud_label"]



