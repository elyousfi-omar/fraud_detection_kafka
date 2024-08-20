import pandas as pd
from sklearn import preprocessing

def feature_engineering(raw_data):
    # Convert column names to lowercase and replace spaces with underscores
    raw_data.columns = raw_data.columns.str.lower().str.replace(" ", "_")
    
    # Extract the cycle number from the 'mc_fileid_number' column
    raw_data["cycle"] = raw_data["mc_fileid_number"].astype(str).str[22:23]
    
    # Extract the first three characters of the 'merchant_country_code' column
    raw_data["merchant_country_code"] = raw_data["merchant_country_code"].astype(str).str[:3]

    # Calculate the mean settlement amount by card_id
    mean_by_cardid = raw_data.groupby('card_id')['settlement_amount'].mean()
    mean_by_cardid = mean_by_cardid.reset_index()
    mean_by_cardid.rename(columns={"settlement_amount": "mean_by_cardid"}, inplace=True)

    # Merge the mean settlement amount back into the raw_data dataframe
    raw_data = raw_data.merge(mean_by_cardid, on="card_id", how='left')

    # Initialize the LabelEncoder
    le = preprocessing.LabelEncoder()

    # Apply label encoding to categorical columns
    raw_data[["transaction_type", "settlement_currency", "merchant_country_code", "transaction_code", "transaction_currency", "payment_provider", "action_code", "cycle"]] = raw_data[["transaction_type", "settlement_currency", "merchant_country_code", "transaction_code", "transaction_currency", "payment_provider", "action_code", "cycle"]].astype(str).apply(le.fit_transform)
    
    # Convert 'transaction_date' to datetime format
    raw_data["transaction_date"] = pd.to_datetime(raw_data["transaction_date"])

    # Extract day, month, and year from 'transaction_date'
    raw_data["day"] = raw_data["transaction_date"].dt.day
    raw_data["month"] = raw_data["transaction_date"].dt.month
    raw_data["year"] = raw_data["transaction_date"].dt.year

    # Select columns to be used for prediction
    pred_cols = ["transaction_amount", "merchant_country_code", "settlement_amount", "transaction_code", "transaction_currency", "settlement_currency", "transaction_type", "payment_provider", "action_code", "cycle", "mean_by_cardid", "day", "month", "year"]
    raw_data = raw_data[pred_cols]
    
    return raw_data