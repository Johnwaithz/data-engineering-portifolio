import psycopg2

def load_data(df):
    conn = psycopg2.connect(
        dbname="data_engineering",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )

    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO crypto_prices 
            (coin_id, symbol, name, price, market_cap, volume)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()
