import pandas as pd


def convert_column_df(df, column_name, csv_path):
    new_df = pd.DataFrame(set(df[column_name].tolist()))
    new_df.to_csv(csv_path)


def save_columns(columns):
    for column in columns:
        convert_column_df(df, column, f"{column}.csv")


df = pd.read_csv("data.csv", encoding='utf-8')


index = (1 - df["isFund"]) * (1 - df["isAdr"]) * (1 - df["isEtf"])
#
active_stocks = df[index == 1]




columns = ["Symbol", "country", "exchange", "exchangeShortName"]

save_columns(columns)
#
# #
unique_combinations = df[['industry', 'sector']].drop_duplicates()
unique_combinations.to_csv('industries.csv', index=False)

# print(df)
# print(df.columns)
#
# countries.to_csv("countries_list.csv")
# exchanges = set(df["exchange"].tolist())


#
# print(f"number of exchanges: {len(exchanges)}")
# sectors = set(df["sector"].tolist())
# print(f"number of sectors: {len(sectors)}")
# sectors = set(df["industry"].tolist())
# print(f"number of industries: {len(sectors)}")
#
# print(f"All points:{len(df)}")
# print(f"All active points:{len(df[df.isActivelyTrading])}")
#
# index = (1 - df["isActivelyTrading"]) * (1 - df["isFund"]) * (1 - df["isAdr"])* (1 - df["isEtf"])
# active_stocks = df[index == 1]
#
# index = (1 - df["isFund"]) * (1 - df["isAdr"])* (1 - df["isEtf"])
#
# stocks = df[index == 1]
#
# print(f"All companies:{len(stocks)}")
# print(f"All active companies:{len(active_stocks)}")