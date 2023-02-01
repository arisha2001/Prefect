import pandas as pd
from prefect import task, flow


@task
def load_file(input: str) -> pd.DataFrame:
    df = pd.read_csv(input)
    return df


@task
def normalize_file(df: pd.DataFrame) -> pd.DataFrame:
    df_column = df['url']
    l = list()
    for data in df_column:
        cur_index = data.find("://")
        cur_str = data[cur_index + 3:]
        cur_str_new = cur_str[:cur_str.find("/")]
        l.append(cur_str_new)
    df['domain_of_url'] = l
    return df


@task
def save_file(df: pd.DataFrame) -> None:
    df.to_csv('./output.csv', index=False)


@flow
def pipeline():
    input_file = './data.csv'
    df = load_file(input_file)
    normalized_df = normalize_file(df)
    save_file(normalized_df)
