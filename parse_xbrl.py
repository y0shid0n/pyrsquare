# 有報のxbrlを気合いでパースする

import pandas as pd
import numpy as np
from edinet_xbrl.edinet_xbrl_parser import EdinetXbrlParser
from pathlib import Path
import re
import csv
from lib import myfunc

# init parser
parser = EdinetXbrlParser()

# file list
data_path = Path("./data")
file_list = data_path.glob("*.xbrl")

# どっちがどっちか要確認
# 単体 or 連結のkeyの辞書
# fs_dict = {"non-consolidated": "jpcrp_cor:NotesTaxEffectAccountingFinancialStatementsTextBlock"
#     , "consolidated": "jpcrp_cor:NotesTaxEffectAccountingConsolidatedFinancialStatementsTextBlock"}
# とりあえず連結だけでok
fs_dict = {"consolidated": "jpcrp_cor:NotesTaxEffectAccountingConsolidatedFinancialStatementsTextBlock"}

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

    for k, v in fs_dict.items():
        print(k)
        #print(v)
        table = myfunc.get_table(obj, v, context_ref)

        # tableがない場合はスキップ
        if table is None:
            # ToDo: loggingでlog出力したい
            print("There is no table.")
            # 空ファイルを出しておく（python3.4以降のみ対応）
            Path("./output/parsed_csv/{}_{}_test.csv".format(ecode, k)).touch()
            continue

        # tableをpd.DataFrameに変更
        table_list = myfunc.table_to_list(table)
        df = myfunc.list_to_pd(table_list)

        # 単位が分かれていて列数が合っているものは単位のカラム名がないので例外処理
        # ある程度パターンがわかったらmyfuncに回してもいいかも
        if ecode == "E04196":
            df.columns = ["account", "blank1", "前連結会計年度(平成30年3月31日)", "前連結会計年度(平成30年3月31日)_unit"
                , "blank2", "blank3", "当連結会計年度(平成31年3月31日)" ,"当連結会計年度(平成31年3月31日)_unit"]
        elif ecode == "E00067":
            df.columns = ["account", "前連結会計年度(2018年3月31日)", "前連結会計年度(2018年3月31日)_unit"
                , "当連結会計年度(2019年3月31日)", "当連結会計年度(2019年3月31日)_unit"]

        # 辞書のキーでカラム名を変える
        if k == "consolidated":
            colname_tmp = "連結会計年度"
        else:
            colname_tmp = "事業年度"

        # # dfが6列の場合は単位のカラムの補完と値の数値変換を行う
        # if len(df.columns) == 6:
        #     df.loc[:, df.columns.str.contains("_unit")] = df.loc[:, df.columns.str.contains("_unit")].apply(myfunc.fill_unit)
        #     # 値のカラム
        #     collist_val = [i for i in df.columns[df.columns.str.contains(colname_tmp)] if re.search("^.*(?<!_unit)$", i)]
        #     df.loc[:, collist_val] = df.loc[:, collist_val].applymap(myfunc.get_value)

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
        df_output["fs_type"] = k
        df_output["acc_standard"] = acc_standard

        output_filename = "./output/parsed_csv/{}_{}_test.csv".format(ecode, k)
        df_output.to_csv(output_filename, sep=",", index=False, encoding="utf-8")
