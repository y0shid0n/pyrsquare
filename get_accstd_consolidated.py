# 全EDINET CODEに対して必要な情報を付加して出力する

import pandas as pd
from edinet_xbrl.edinet_xbrl_parser import EdinetXbrlParser
from pathlib import Path
import gc
import re
from lib import myfunc

# init parser
parser = EdinetXbrlParser()

# file list
# skipも見に行くため、再帰的に探索する
data_path = Path("./data")
file_list = data_path.glob("**/*.xbrl")

# 出力ファイル名
output_file = "./output/ecode_info_all.csv"

# 1行目の出力
with open(output_file, "w") as f:
    f.write("ecode,acc_standard,cosolidated_flg,loss_carry_forward,tax_rate_diff\n")

def get_result(file):
    # edinet codeの取得
    ecode = myfunc.get_ecode(str(file))

    # xbrlをパースしたオブジェクトを作成
    obj = parser.parse_file(file)

    # 会計基準の取得
    acc_std = obj.get_data_by_context_ref("jpdei_cor:AccountingStandardsDEI", "FilingDateInstant").get_value()

    # 連結ありなしの取得
    consolidated_flg = obj.get_data_by_context_ref("jpdei_cor:WhetherConsolidatedFinancialStatementsArePreparedDEI", "FilingDateInstant").get_value()

    # 表のオブジェクトを取得
    soup = myfunc.get_html(obj, "jpcrp_cor:NotesTaxEffectAccountingConsolidatedFinancialStatementsTextBlock", "CurrentYearDuration")

    # 表のオブジェクトから繰越欠損金の回収予定表の有無を取得
    loss_carry_forward = str(myfunc.get_string(soup, re.compile(r".*繰越欠損金.*繰越期限.*")))

    # 表のオブジェクトから税率差異の説明表の有無を取得
    tax_rate_diff = str(myfunc.get_string(soup, re.compile(r".実効税率.*税効果会計.*重要な差異.*")))

    # リストを作成してoutput_allに追加
    result = [ecode, acc_std, consolidated_flg, loss_carry_forward, tax_rate_diff]

    with open(output_file, "a") as f:
        f.write(",".join(result) + "\n")

# 全ファイルを実行
output = [get_result(i) for i in file_list]
