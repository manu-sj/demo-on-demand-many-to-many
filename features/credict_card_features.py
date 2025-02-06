from hopsworks import udf
from datetime import datetime

@udf([int, int, int, str, str, str], drop=["current_datetime"], mode='python')
def fetch_and_create_credit_card_features(cc_num : int, 
                                          current_datetime : datetime, 
                                          context: dict):
    """
    Transformation function that uses storage connector to fetch data from postgres and create on-demand features from the data.
    """
    conn = context["connection"]
    
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM credit_cards WHERE cc_num = %s", (cc_num,))
        cc_num, data = cur.fetchone()

    # Compute on-demand features
    data["days_to_expiry"] = (current_datetime - datetime.strptime(data["expires"], "%m/%y")).days

    data["age_at_transaction"] = (current_datetime - datetime.strptime(data["birthdate"], "%Y-%m-%d")).days


    # Return on-demand feature as well as other features required
    return cc_num, data["days_to_expiry"], data["age_at_transaction"], data["sex"], data["City"], data["Country"]



