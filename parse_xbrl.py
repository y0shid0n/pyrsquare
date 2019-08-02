# 有報のxbrlを気合いでパースする

import pandas as pd
import numpy as np
from edinet_xbrl.edinet_xbrl_parser import EdinetXbrlParser
from pathlib import Path
import re
import csv
from lib import myfunc

## init parser
parser = EdinetXbrlParser()

# file list
data_path = Path("./data")
file_list = data_path.glob("*.xbrl")

## parse xbrl file and get data container
xbrl_file_path = "./data/jpcrp030000-asr-001_E00008-000_2019-03-31_01_2019-06-21.xbrl"
edinet_xbrl_object = parser.parse_file(xbrl_file_path)

# どっちがどっちか要確認
# 単体 or 連結のkeyの辞書
fs_dict = {"not_cons": "jpcrp_cor:NotesTaxEffectAccountingFinancialStatementsTextBlock"
    , "cons": "jpcrp_cor:NotesTaxEffectAccountingConsolidatedFinancialStatementsTextBlock"}

context_ref = "CurrentYearDuration"

for file in file_list:
    print(file)
    ecode = myfunc.get_ecode(str(file))
    for k, v in fs_dict.items():
        print(k)
        print(v)
        obj = parser.parse_file(file)
        table = myfunc.get_table(obj, v, context_ref)
        df = myfunc.table_to_pd(table)

        # 辞書のキーでカラム名を変える
        if k == "cons":
            colname_tmp = "連結会計年度"
        else:
            colname_tmp = "事業年度"

        # dfが6列の場合は単位のカラムの補完と値の数値変換を行う
        if len(df.columns) == 6:
            df.loc[:, df.columns.str.contains("_unit")] = df.loc[:, df.columns.str.contains("_unit")].apply(myfunc.fill_unit)
            # 値のカラム
            collist_val = [i for i in df.columns[df.columns.str.contains(colname_tmp)] if re.search("^.*(?<!_unit)$", i)]
            df.loc[:, collist_val] = df.loc[:, collist_val].applymap(myfunc.get_value)

        # dfが4列の場合は単位の分離を行う
        if len(df.columns) == 4:
            # colname_tmpが含まれるカラムを取得
            colname_tmp_list = [i for i in df.columns if re.search(colname_tmp, i)]

            for col in colname_tmp_list:
                df = myfunc.sep_unit(df, col)

        # edinet codeをカラムとして持たせる
        df["ecode"] = ecode

        output_filename = "./output/{}_{}_test.csv".format(ecode, k)
        df.to_csv(output_filename, sep=",", index=False, encoding="utf-8")
