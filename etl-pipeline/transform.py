import pandas as pd

def transform_data(data):
    df = pd.DataFrame(data)

    df = df[[
        "id",
        "symbol",
        "name",
        "current_price",
        "market_cap",
        "total_volume"
    ]]

    df.columns = [
        "coin_id",
        "symbol",
        "name",
        "price",
        "market_cap",
        "volume"
    ]

    return df
