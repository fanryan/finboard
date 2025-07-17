import os
import json
import pandas as pd

def load_json_to_df(filepath):
    with open(filepath, "r") as f:
        data = json.load(f)
    return pd.DataFrame(data)

def safe_divide(n, d):
    return n / d.replace({0: pd.NA})

def clean_income_statement(df):
    key_columns = [
        "date", "calendarYear", "period", "revenue", "costOfRevenue", "grossProfit",
        "operatingExpenses", "researchAndDevelopmentExpenses", "sellingGeneralAndAdministrativeExpenses",
        "interestExpense", "incomeBeforeTax", "incomeTaxExpense", "netIncome", "eps",
        "weightedAverageShsOut", "finalLink"
    ]
    available_columns = [col for col in key_columns if col in df.columns]
    df_clean = df[available_columns].copy()

    if 'date' in df_clean.columns:
        df_clean['date'] = pd.to_datetime(df_clean['date'])
        df_clean = df_clean.sort_values(by='date', ascending=False)

    df_clean['grossMargin'] = safe_divide(df_clean.get('grossProfit', 0), df_clean.get('revenue', 0))
    df_clean['operatingMargin'] = safe_divide(
        df_clean.get('grossProfit', 0) - df_clean.get('operatingExpenses', 0),
        df_clean.get('revenue', 0)
    )
    df_clean['netProfitMargin'] = safe_divide(df_clean.get('netIncome', 0), df_clean.get('revenue', 0))
    df_clean['ebitdaMargin'] = safe_divide(df_clean.get('ebitda', 0), df_clean.get('revenue', 0))
    df_clean['rndIntensity'] = safe_divide(df_clean.get('researchAndDevelopmentExpenses', 0), df_clean.get('revenue', 0))
    df_clean['sgnaIntensity'] = safe_divide(df_clean.get('sellingGeneralAndAdministrativeExpenses', 0), df_clean.get('revenue', 0))
    df_clean['interestExpenseRatio'] = safe_divide(df_clean.get('interestExpense', 0), df_clean.get('revenue', 0))
    df_clean['incomeBeforeTaxMargin'] = safe_divide(df_clean.get('incomeBeforeTax', 0), df_clean.get('revenue', 0))
    df_clean['effectiveTaxRate'] = safe_divide(df_clean.get('incomeTaxExpense', 0), df_clean.get('incomeBeforeTax', 0))
    df_clean['interestCoverage'] = safe_divide(df_clean.get('incomeBeforeTax', 0), df_clean.get('interestExpense', 0))

    return df_clean

def clean_balance_sheet(df):
    key_columns = [
        "date", "calendarYear", "period", "cashAndCashEquivalents", "shortTermInvestments",
        "totalCurrentAssets", "inventory", "netReceivables", "propertyPlantEquipmentNet",
        "goodwillAndIntangibleAssets", "totalAssets", "accountPayables", "shortTermDebt",
        "totalCurrentLiabilities", "longTermDebt", "totalNonCurrentLiabilities", "totalLiabilities",
        "commonStock", "retainedEarnings", "totalStockholdersEquity",
        "totalLiabilitiesAndStockholdersEquity", "totalDebt", "netDebt", "finalLink"
    ]
    available_cols = [col for col in key_columns if col in df.columns]
    df_clean = df[available_cols].copy()

    if 'date' in df_clean.columns:
        df_clean['date'] = pd.to_datetime(df_clean['date'])
        df_clean = df_clean.sort_values(by='date', ascending=False)

    df_clean["currentRatio"] = safe_divide(df_clean.get("totalCurrentAssets", 0), df_clean.get("totalCurrentLiabilities", 0))
    df_clean["cashRatio"] = safe_divide(
        df_clean.get("cashAndCashEquivalents", 0) + df_clean.get("shortTermInvestments", 0),
        df_clean.get("totalCurrentLiabilities", 0)
    )
    df_clean["debtToEquity"] = safe_divide(df_clean.get("totalDebt", 0), df_clean.get("totalStockholdersEquity", 0))
    df_clean["netDebtToAssets"] = safe_divide(df_clean.get("netDebt", 0), df_clean.get("totalAssets", 0))
    df_clean["workingCapital"] = df_clean.get("totalCurrentAssets", 0) - df_clean.get("totalCurrentLiabilities", 0)
    df_clean["inventoryRatio"] = safe_divide(df_clean.get("inventory", 0), df_clean.get("totalAssets", 0))

    return df_clean

def clean_cash_flow(df):
    key_columns = [
        "date", "calendarYear", "period", "netIncome", "depreciationAndAmortization",
        "stockBasedCompensation", "changeInWorkingCapital", "netCashProvidedByOperatingActivities",
        "investmentsInPropertyPlantAndEquipment", "netCashUsedForInvestingActivites",
        "commonStockRepurchased", "dividendsPaid", "netCashUsedProvidedByFinancingActivities",
        "capitalExpenditure", "freeCashFlow", "cashAtBeginningOfPeriod", "cashAtEndOfPeriod", "finalLink"
    ]
    available_cols = [col for col in key_columns if col in df.columns]
    df_clean = df[available_cols].copy()

    if 'date' in df_clean.columns:
        df_clean['date'] = pd.to_datetime(df_clean['date'])
        df_clean = df_clean.sort_values(by='date', ascending=False)

    df_clean["fcfToNetIncome"] = safe_divide(df_clean.get("freeCashFlow", 0), df_clean.get("netIncome", 0))
    df_clean["capexToOperatingCash"] = safe_divide(df_clean.get("capitalExpenditure", 0).abs(), df_clean.get("netCashProvidedByOperatingActivities", 0))
    df_clean["dividendCoverage"] = safe_divide(df_clean.get("netCashProvidedByOperatingActivities", 0), df_clean.get("dividendsPaid", 0).abs())
    df_clean["repurchaseToFreeCashFlow"] = safe_divide(df_clean.get("commonStockRepurchased", 0).abs(), df_clean.get("freeCashFlow", 0))
    df_clean["fcfConversion"] = safe_divide(df_clean.get("freeCashFlow", 0), df_clean.get("netIncome", 0))

    return df_clean

def merge_financials(income_df, balance_df, cashflow_df):
    merged_df = income_df.merge(balance_df, on="date", how="outer", suffixes=("_income", "_balance"))
    merged_df = merged_df.merge(cashflow_df, on="date", how="outer")
    merged_df = merged_df.sort_values(by="date", ascending=False).reset_index(drop=True)

    merged_df["roa"] = safe_divide(merged_df.get("netIncome", merged_df.get("netIncome_income", 0)), merged_df.get("totalAssets"))
    merged_df["roe"] = safe_divide(merged_df.get("netIncome", merged_df.get("netIncome_income", 0)), merged_df.get("totalStockholdersEquity"))
    
    # Drop redundant columns
    cols_to_drop = [
        col for col in merged_df.columns if (
            col.startswith("calendarYear_") or col.startswith("period_") or col.startswith("finalLink_") or
            col.startswith("finalLink") or col == "period" or col == "finalLink"
        )
    ]
    merged_df.drop(columns=cols_to_drop, inplace=True, errors='ignore')

    # Move calendarYear to front if exists
    if "calendarYear" in merged_df.columns:
        cols = merged_df.columns.tolist()
        cols.remove("calendarYear")
        merged_df = merged_df[["calendarYear"] + cols]

    return merged_df

def save_processed_data(dfs: dict, symbol: str):
    out_folder = os.path.join("data", "processed", symbol)
    os.makedirs(out_folder, exist_ok=True)
    paths = {}
    for name, df in dfs.items():
        path = os.path.join(out_folder, f"{name}.csv")
        df.to_csv(path, index=False)
        paths[name] = path
        print(f"Saved {name} to {path}")
    return paths

def main(symbol: str):
    raw_folder = os.path.join("data", "raw", symbol)

    income_path = os.path.join(raw_folder, "income-statement.json")
    balance_path = os.path.join(raw_folder, "balance-sheet-statement.json")
    cashflow_path = os.path.join(raw_folder, "cash-flow-statement.json")

    income_df = load_json_to_df(income_path)
    balance_df = load_json_to_df(balance_path)
    cashflow_df = load_json_to_df(cashflow_path)

    income_clean = clean_income_statement(income_df)
    balance_clean = clean_balance_sheet(balance_df)
    cashflow_clean = clean_cash_flow(cashflow_df)

    merged_df = merge_financials(income_clean, balance_clean, cashflow_clean)

    processed_dfs = {
        "income_clean": income_clean,
        "balance_clean": balance_clean,
        "cashflow_clean": cashflow_clean,
        "merged_financials": merged_df
    }

    save_processed_data(processed_dfs, symbol)

if __name__ == "__main__":
    import sys
    sym = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
    main(sym)