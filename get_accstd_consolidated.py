# 全EDINET CODEの会計基準と連結ありなしを取得する

import pandas as pd
from edinet_xbrl.edinet_xbrl_parser import EdinetXbrlParser
from pathlib import Path
import gc
from lib import myfunc

# init parser
parser = EdinetXbrlParser()

# file list
# skipも見に行くため、再帰的に探索する
data_path = Path("./data")
file_list = data_path.glob("**/*.xbrl")

# 出力ファイル名
output_file = "./output/accstd_consolidated_all.csv"

# 1行目の出力
with open(output_file, "w") as f:
    f.write("ecode,acc_standard,cosolidated_flg\n")

def get_result(file):
    # edinet codeの取得
    ecode = myfunc.get_ecode(str(file))

    # xbrlをパースしたオブジェクトを作成
    obj = parser.parse_file(file)

    # 会計基準の取得
    acc_std = obj.get_data_by_context_ref("jpdei_cor:AccountingStandardsDEI", "FilingDateInstant").get_value()

    # 連結ありなしの取得
    consolidated_flg = obj.get_data_by_context_ref("jpdei_cor:WhetherConsolidatedFinancialStatementsArePreparedDEI", "FilingDateInstant").get_value()

    # リストを作成してoutput_allに追加
    result = [ecode, acc_std, consolidated_flg]

    with open(output_file, "a") as f:
        f.write(",".join(result) + "\n")

# 全ファイルを実行
output  = [get_result(i) for i in file_list]

