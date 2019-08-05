# 有報のxbrlを気合いでパースする

import pandas as pd
import numpy as np
from edinet_xbrl.edinet_xbrl_parser import EdinetXbrlParser
from pathlib import Path
import re
import csv
import sys
import os
from lib import myfunc

# 引数を取得
args = sys.argv

# 処理済みファイル名
checked_file = "./output/checked_file.txt"

# 引数がなければそのまま、引数にinitがあれば処理済リストを初期化
if not Path(checked_file).exists():
    Path(checked_file).touch()
elif len(args) == 1:
    pass
elif args[1] == "init":
    os.remove(checked_file)
    Path(checked_file).touch()

# checked_fileの読み込み
with open(checked_file, "r", encoding="UTF-8") as f:
    checked_file_list = [i.strip() for i in f.readlines()]

# init parser
parser = EdinetXbrlParser()

# file list
data_path = Path("./data")
file_list = data_path.glob("*.xbrl")
# 処理済みとの差分
file_list = [i for i in file_list if i.name not in checked_file_list]

# どっちがどっちか要確認
# 単体 or 連結のkeyの辞書
# fs_dict = {"non-consolidated": "jpcrp_cor:NotesTaxEffectAccountingFinancialStatementsTextBlock"
#     , "consolidated": "jpcrp_cor:NotesTaxEffectAccountingConsolidatedFinancialStatementsTextBlock"}

# とりあえず連結だけでokなのでループを削除
# ToDo: fs_typeを引数から取るのでもいいかも
fs_dict = {"consolidated": "jpcrp_cor:NotesTaxEffectAccountingConsolidatedFinancialStatementsTextBlock"}
fs_type = "consolidated"
target_key = fs_dict[fs_type]

context_ref = "CurrentYearDuration"

# ファイルを読み込んで処理
# ToDo: 処理時間にもよるが遅かったらmultiprocessingなどでやるのを検討
# 出力はpickleとかにしたほうがいいかも
for file in file_list:
    print(file)
    # edinet codeの取得
    ecode = myfunc.get_ecode(str(file))

    # xbrlをパースしたオブジェクトを作成
    obj = parser.parse_file(file)

    # 会計基準の取得
    acc_standard = myfunc.get_acc_standard(obj)

    # tableタグの抽出
    table = myfunc.get_table(obj, target_key, context_ref)

    # tableがない場合はスキップ
    if table is None:
        # ToDo: loggingでlog出力したい
        print("There is no table.")
        # 空ファイルを出しておく（python3.4以降のみ対応）
        Path("./output/parsed_csv/{}_{}_test.csv".format(ecode, fs_type)).touch()
        # checked_fileに書き込み
        with open(checked_file, "a", encoding="UTF-8") as f:
            f.write(file.name + "\n")
        continue

    # tableをpd.DataFrameに変更
    table_list = myfunc.table_to_list(table)
    # ToDo: リストの長さが変で後ろの処理で吸収できないものは、list_to_pdに渡す前に個別処理を行う
    # nested listをdataframeに変換する
    df = myfunc.list_to_pd(table_list)

    # 単位が分かれていて列数が合っているものは単位のカラム名がないので例外処理
    df = myfunc.modify_df_individual(df, ecode)

    # 辞書のキーでカラム名を変える
    if fs_type == "consolidated":
        colname_tmp = "連結会計年度"
    else:
        colname_tmp = "事業年度"

    # dfが4列の場合は単位の分離を行う
    if len(df.columns) <= 4:
        # colname_tmpが含まれるカラムを取得
        colname_tmp_list = [i for i in df.columns if re.search(colname_tmp, i)]

        for col in colname_tmp_list:
            df = myfunc.sep_unit(df, col)

    # カラム名から期間の情報を抽出して別のカラムにし、カラム名からは期間を削除
    df_output = myfunc.sep_period(df)

    # 単位のカラムの補完と値の数値変換
    df_output.loc[:, df_output.columns.str.contains("_unit")] = df_output.loc[:, df_output.columns.str.contains("_unit")].apply(myfunc.fill_unit)
    # 値のカラムを数値にする
    #df_output.loc[:, ["cur_value", "prev_value"]] = df_output.loc[:, ["cur_value", "prev_value"]].applymap(myfunc.get_value)
    df_output["cur_value"] = df_output[["account", "cur_value"]].apply(lambda x: myfunc.get_value(x[0], x[1]), axis=1)
    df_output["prev_value"] = df_output[["account", "prev_value"]].apply(lambda x: myfunc.get_value(x[0], x[1]), axis=1)

    # edinet code, 会計基準, 単体or連結をカラムとして持たせる
    df_output["ecode"] = ecode
    df_output["fs_type"] = fs_type
    df_output["acc_standard"] = acc_standard

    # 全カラムの前後にある空白文字を削除
    df_output = df_output.applymap(lambda x: x.strip())

    output_filename = "./output/parsed_csv/{}_{}_test.csv".format(ecode, fs_type)
    df_output.to_csv(output_filename, sep=",", index=False, encoding="utf-8")

    # 処理済みファイルに書き込み
    with open(checked_file, "a", encoding="UTF-8") as f:
        f.write(file.name + "\n")
