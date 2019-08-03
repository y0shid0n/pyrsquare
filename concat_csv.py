# パースして出力したcsvをまとめる

import pandas as pd
from pathlib import Path
from lib import myfunc

# ファイルリスト
parsed_csv_path = Path("./output/parsed_csv/")
parsed_csv_path_list = parsed_csv_path.glob("*.csv")

# ファイルを読み込んでリストへ
# ToDo: 処理時間にもよるが遅かったらmultiprocessingなどでやるのを検討
df_list = [myfunc.load_tax_effect_csv(f) for f in parsed_csv_path_list]

# dataframeの結合
# concatはpandasのバージョンが古いとsortがない
df_all = pd.concat(df_list, ignore_index=True)

# 出力
df_all.to_csv("./output/tax_effect_all.csv", sep=",", index=False, encoding="utf-8")
