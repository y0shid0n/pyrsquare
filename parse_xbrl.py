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

# 会社リストの読み込み
# 今回は会社リストに存在する会社のうち、上場会社のみを使用する
company_list = pd.read_csv("./csv/company_list.tsv", sep="\t", encoding="utf-8")
target_company = list(company_list.query("取引所!='非上場'")["EDINETコード"])

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
    # 1つ目のテーブルに単位のみが存在する場合の判定用フラグ
    unit_tmp_flg = 0

    # 何番目のtableタグまで拾ったかのカウント
    table_cnt = 0

    # edinet codeの取得
    ecode = myfunc.get_ecode(str(file))

    # 出力ファイル名
    output_file = "./output/parsed_csv/{}_{}_test.csv".format(ecode, fs_type)

    # edinet codeが分析対象リストにない場合はスキップ
    if ecode not in target_company:
        print("{} is not target company.".format(ecode))
        myfunc.skip_get_data(file, checked_file, output_file)
        continue

    # xbrlをパースしたオブジェクトを作成
    obj = parser.parse_file(file)

    # 会計基準の取得
    acc_standard = obj.get_data_by_context_ref("jpdei_cor:AccountingStandardsDEI", "FilingDateInstant").get_value()

    # 該当箇所のhtmlをパース
    soup = myfunc.get_html(obj, target_key, context_ref)

    # tableタグの抽出
    if soup is None:
        # ToDo: loggingでlog出力したい
        print("There is no table.")
        myfunc.skip_get_data(file, checked_file, output_file)
        continue
    else:
        table = soup.findAll("table")[0]
        table_cnt += 1

    # tableをpd.DataFrameに変更
    table_list = myfunc.table_to_list(table)

    # 1つ目のtableタグで何も取れなかった時の対応
    if len(table_list) == 0:
        table = soup.findAll("table")[1]
        table_list = myfunc.table_to_list(table)
        table_cnt += 1

    # tableタグが1行目で切れている場合の対応
    if len(table_list) == 1:
        check_title = [True if re.search(".*繰延税金.*原因.*内訳", i) else False for i in table_list[0]]
        check_unit = [True if re.search(".*単位.*", i) else False for i in table_list[0]]
        check_year = [True if re.search(".*年.*月.*日", i) else False for i in table_list[0]]
        if any(check_title):
            table_list = myfunc.table_to_list(soup.findAll("table")[1])
            table_cnt += 1
        elif any(check_unit):
            unit_tmp_flg = 1
            unit_tmp = "".join(table_list[0]).replace("単位", "").replace(":", "").replace("：", "").strip()
            table_list = myfunc.table_to_list(soup.findAll("table")[1])
            table_cnt += 1
        elif any(check_year):
            table_list_tmp = myfunc.table_to_list(soup.findAll("table")[1])
            table_list.extend(table_list_tmp)
            table_cnt += 1

    # 表の途中でtableタグが切れている場合の対応
    # table_list最後の行が"繰延税金資産合計"の場合は次も拾う
    check_last = [True if re.sub("\s+", "", i) == "繰延税金資産合計" else False for i in table_list[-1]]
    if any(check_last) and ecode not in ["E32161", "E05283", "E34165", "E00471", "E05674", "E05432", "E30130", "E05302", "E05543", "E04804", "E00288", "E30993", "E33238", "E32159", "E05573", "E03728"]:
        table_list_tmp = myfunc.table_to_list(soup.findAll("table")[table_cnt])
        # table_list_tmpの頭に年度の行があった場合は削除
        check_year_2nd = [True if re.search(".*年.*月.*日", i) else False for i in table_list_tmp[0]]
        if any(check_year_2nd):
            table_list_tmp = table_list_tmp[1:]
        table_list.extend(table_list_tmp)
        table_cnt += 1

    # ToDo: リストの長さが変で後ろの処理で吸収できないものは、list_to_pdに渡す前に個別処理を行う
    if ecode == "E04346":
        table_list[0] = [i for i in table_list[0] if i != ""]
        table_list[0].insert(0, "account")
        table_list[0].insert(-1, "blank")

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

    # 1個目のテーブルに単位のみがあった場合の処理
    if unit_tmp_flg == 1:
        df_output["cur_value_unit"] = unit_tmp
        df_output["prev_value_unit"] = unit_tmp

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

    # 全カラムの前後にある空白文字を削除（文字列カラムのみ処理）
    df_output = df_output.applymap(lambda x: x.strip() if type(x) is str else x)

    # accountは間にスペースが入っている場合があるので削除
    df_output["account"] = df_output["account"].apply(lambda x: re.sub(r"\s+", "", x))

    df_output.to_csv(output_file, sep=",", index=False, encoding="utf-8")

    # 処理済みファイルに書き込み
    with open(checked_file, "a", encoding="UTF-8") as f:
        f.write(file.name + "\n")
