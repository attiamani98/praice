
def plot_total_sales(df):
    (
        df
        .loc[lambda df: df.timestamp > "2024-03-29 12:30:00"]
        .groupby('timestamp')
        .agg(sales = ('sales', 'sum'))
        .plot(title="Sales over time", ylabel="Sales", xlabel="Timestamp")
    )
