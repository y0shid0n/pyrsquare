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

# 出力用のリスト
#output_all = []

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
    #output_all.append([ecode, acc_std, consolidated_flg])
    result = [ecode, acc_std, consolidated_flg]

    # メモリを空ける
    del obj
    gc.collect()

    return result

# 全ファイルを実行
output_all = [get_result(i) for i in file_list]

# output_allをcsvに出力
output_df = pd.DataFrame(output_all, columns = ["ecode", "acc_standard", "consolidated_flg"])
output_df.to_csv(output_file, sep="\t", index=False, encoding="utf-8")
