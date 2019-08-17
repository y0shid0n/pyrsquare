# 関数定義
# あとでクラスにしたほうがいいかも

import re
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import jaconv
from datetime import datetime as dt
import sys
from pprint import pprint

def get_ecode(filename):
    """
    ファイル名からedinet codeを取得する
    """
    ecode = re.search("(?<=_)E[0-9]+(?=-)", filename)
    return ecode.group(0)

def skip_get_data(target_file, checked_file, output_file):
    """
    スキップ時の処理
    """
    # 空ファイルを出しておく（python3.4以降のみ対応）
    Path(output_file).touch()
    # checked_fileに書き込み
    with open(checked_file, "a", encoding="UTF-8") as f:
        f.write(target_file.name + "\n")
    continue

def get_html(obj, key, context_ref):
    """
    ファイルから必要な部分のhtmlをパース
    """
    # keyとcontext_refから該当箇所を取得
    current_year_assets = obj.get_data_by_context_ref(key, context_ref)

    # 該当箇所がない場合はNoneを返す
    if current_year_assets is None:
        return None

    # htmlをパース
    soup = BeautifulSoup(current_year_assets.get_value(), 'html.parser')

    return soup

def get_string(soup, pattern):
    """
    html内の特定文字列の有無を返す関数
    """
    if soup is None:
        result_list = []
    else:
        result_list = soup.find_all(string = pattern)

    if len(result_list) == 0:
        result = 0
    elif len(result_list) >= 2:
        result = 2
    else:
        result = 1

    return result

def table_to_list(table):
    """
    htmlのtableをpd.DataFrameに変換する
    """
    # tableのtrタグを取得
    rows = table.findAll("tr")

    # テーブル全体を格納するリスト
    result_list = []

    for row in rows:
        csvRow = []
        for cell in row.findAll(['td', 'th']):
            tmp = cell.get_text().replace("\n", "").replace("\xa0", "")
            csvRow.append(tmp)
        result_list.append(csvRow)

    # 全て空または"繰延税金資産"のみまたは"繰延税金負債"のみの行は削除
    result_list = [i for i in result_list\
        if i.count("") + i.count("繰延税金資産") + i.count("繰延税金負債")\
            + i.count("（繰延税金資産）") + i.count("（繰延税金負債）")\
            + i.count("(繰延税金資産)") + i.count("(繰延税金負債)") != len(i)]

    # 削除した結果何もなくなった場合は空のリストを返す
    if result_list == []:
        pprint(result_list)
        return result_list

    # 最後に表示方法の変更が入っていたら削除
    if "表示方法の変更" in result_list[-1][0]:
        result_list = result_list[:-1]
    # 最後に増加の主な内容が入っていたら削除
    elif "この増加の主な内容は" in result_list[-1][0]:
        result_list = result_list[:-1]

    pprint(result_list)
    return(result_list)

def delete_empty_columns(result_df):
    """
    全て空文字かつカラム名も空文字の列を削除
    """
    new_colname = []
    for colname, item in result_df.iteritems():
        if all([x == "" for x in list(item)]) and colname == "":
            new_colname.append("drop_tmp")
            #result_df.columns = list(result_df.columns)[:-1] + ["drop_tmp"]
        else:
            new_colname.append(colname)
    # カラム名を書き換えて、"drop_tmp"があれば削除
    result_df.columns = new_colname
    if "drop_tmp" in result_df.columns:
        result_df.drop("drop_tmp", axis=1, inplace=True)

    return result_df

def list_to_pd(result_list):
    """
    htmlのtableタグをパースして作成したリストをpd.DataFrameに変換する
    """
    # 単位のカラムが分かれてる場合とそうでない場合があるので分岐
    # 列数が合うものは単位がわかれていない（一部例外あり）
    col_num_list = [len(i) for i in result_list]
    print(col_num_list)

    if col_num_list.count(max(col_num_list)) == len(col_num_list):
        check_unit = [True if re.search(".*単位.*", i) else False for i in result_list[0]]
        check_year = [True if re.search(".*年.*月.*日", i) else False for i in result_list[1]]
        # 単位が1行目にあるパターンの処理
        if any(check_unit):
            unit_tmp = "".join(result_list[0]).replace("単位", "").replace(":", "").replace("：", "").strip()
            result_list = result_list[1:]
        # 年度が2行目にあるパターンの処理
        elif any(check_year):
            result_list[1] = [i + j for i, j in zip(result_list[0], result_list[1])]
            result_list = result_list[1:]
        # 単位が分かれていなければそのままDataFrameに
        result_df = pd.DataFrame(result_list[1:], columns=result_list[0])
        # 全て空文字かつカラム名も空文字の列を削除
        result_df = delete_empty_columns(result_df)

        # 前期データがない場合は前期データを作成する
        if len(result_df.columns) == 2:
            result_df["前連結会計年度(brank)"] = ""

        # 1行目に単位があった場合は単位のカラムを追加
        if any(check_unit):
            # 1行目から取得した単位を入れる
            result_df["cur_value_unit"] = unit_tmp
            result_df["prev_value_unit"] = unit_tmp

    # 2行パターンの処理
    elif max(col_num_list) > col_num_list[0] and max(col_num_list) > col_num_list[1]:
        result_df = modify_list_individual(result_list)

    # 1行パターンの処理
    else:
        # 単位のカラム名が分かれている場合はカラム名をつける
        # この辺はうまくいかない可能性がありそう（どこに空白列があるかがわからないので）
        result_df = modify_colname_individual(result_list)

    # 全て空文字かつカラム名も空文字の列を削除
    result_df = delete_empty_columns(result_df)

    # カラム名の全角半角の揺れをなくす
    # カラム名の先頭に空白文字がある場合があるので削除
    result_df.columns = [jaconv.z2h(i.strip(), digit=True, ascii=True, kana=False) for i in result_df.columns]

    # 1列目のカラム名はaccountにして空白文字削除、全角記号と英数字は半角に統一
    result_df.columns = ["account"] + list(result_df.columns)[1:]
    #result_df["account"] = result_df["account"].str.strip()
    result_df["account"] = result_df["account"].apply(lambda x: jaconv.z2h(x.strip(), digit=True, ascii=True, kana=False))

    return result_df

def modify_list_individual(table_list):
    """
    上2行の要素数が少ないパターンを処理する
    """
    check_empty = [True if i.strip() == "" else False for i in table_list[1]]
    check_unit = [True if re.search(".*単位.*", i) else False for i in table_list[0]]
    check_year = [True if re.search(".*年.*月.*日", i) else False for i in table_list[1]]

    # 2行目が全て空の場合
    if all(check_empty):
        table_list.pop(1)
    # 1行目に単位がある場合
    elif any(check_unit):
        unit = "".join(table_list[0]).replace("単位", "").replace(":", "").replace("：", "").strip()
        table_list = table_list[1:]
    # カラム名が2行に分かれている場合
    elif len(table_list[0]) == len(table_list[1]) and any(check_year):
        table_list[1] = [i + j for i, j in zip(table_list[0], table_list[1])]
        table_list = table_list[1:]

    # とりあえず差が2のものだけを想定
    # "年度"を含む要素のindexのリストを取得
    value_index_list = [i for i, value in enumerate(table_list[0]) if re.search("年度", value)]
    # そのindexの後ろに、その要素+"_unit"の要素を入れる
    table_list[0].insert(value_index_list[0] + 1, table_list[0][value_index_list[0]] + "_unit")
    table_list[0].insert(value_index_list[1] + 2, table_list[0][value_index_list[1] + 1] + "_unit")

    if any(check_unit):
        # 1行目から取得した単位を入れる
        table_list[2] = [unit if i == table_list[2][value_index_list[0]] + "_unit" else i for i in table_list[2]]
        #table_list[2][[value_index_list[0]] + "_unit"] = unit
        table_list[2] = [unit if i == table_list[2][value_index_list[1] + 1] + "_unit" else i for i in table_list[2]]
        #table_list[2][[value_index_list[1] + 1] + "_unit"] = unit

    result_df = pd.DataFrame(table_list[1:], columns=table_list[0])

    return result_df

def modify_colname_individual(table_list):
    """
    単位のカラム名が分かれている場合はカラム名をつける
    この辺はうまくいかない可能性がありそう（どこに空白列があるかがわからないので）
    時々変なパターンが含まれるので、個別に処理する

    Parameters
    ----------
    table_list : list
        htmlのtableタグからパースして2次元リスト型になっているデータ

    Returns
    -------
    result_df : pd.DataFrame
        table_listをpd.DataFrameに変換したもの
    """
    # リストの各要素をカウント
    col_num_list = [len(i) for i in table_list]
    # カラム名になる1行目の空白文字を削除
    table_list[0] = [i.strip() for i in table_list[0]]

    # 各パターンごとにカラム名となる部分を修正する
    if max(col_num_list) == 3 and len(table_list[0]) == 2:
        table_list[0].append(table_list[0][-1] + "_unit")
        result_df = pd.DataFrame(table_list[1:], columns=table_list[0])
        result_df["前連結会計年度(brank)"] = ""
        result_df["前連結会計年度(brank)_unit"] = ""
    elif max(col_num_list) == 5 and len(table_list[0]) == 4:
        table_list[0].append(table_list[0][-1] + "_unit")
        result_df = pd.DataFrame(table_list[1:], columns=table_list[0])
        result_df[table_list[0][1] + "_unit"] = result_df[table_list[0][1]].apply(lambda x: get_unit(x))
    elif max(col_num_list) == 4 and len(table_list[0]) == 3:
        table_list[0].insert(1, "account")
        result_df = pd.DataFrame(table_list[1:], columns=table_list[0])
    elif max(col_num_list) == 9 and len(table_list[0]) == 3:
        table_list[0].insert(2, table_list[0][1] + "_unit")
        table_list[0].append(table_list[0][-1] + "_unit")
        table_list[0].insert(1, "blank1")
        table_list[0].insert(4, "blank2")
        table_list[0].insert(4, "blank3")
        table_list[0].append("blank4")
        result_df = pd.DataFrame(table_list[1:], columns=table_list[0])
    elif max(col_num_list) == 6 and len(table_list[0]) == 3:
        table_list[0].insert(2, table_list[0][1] + "_unit")
        table_list[0].append(table_list[0][-1] + "_unit")
        table_list[0].insert(1, "blank1")
        result_df = pd.DataFrame(table_list[1:], columns=table_list[0])
    elif max(col_num_list) == 7 and len(table_list[0]) == 4:
        table_list[0].insert(2, table_list[0][1] + "_unit")
        table_list[0].append(table_list[0][-1] + "_unit")
        table_list[0].insert(3, "blank1")
        result_df = pd.DataFrame(table_list[1:], columns=table_list[0])
    elif max(col_num_list) == 9 and len(table_list[0]) == 5:
        table_list[0].insert(3, table_list[0][2] + "_unit")
        table_list[0].append(table_list[0][-1] + "_unit")
        table_list[0].insert(1, "blank1")
        table_list[0].insert(5, "blank2")
        result_df = pd.DataFrame(table_list[1:], columns=table_list[0])
    elif max(col_num_list) - len(table_list[0]) == 2:
        # "年度"を含む要素のindexのリストを取得
        value_index_list = [i for i, value in enumerate(table_list[0]) if re.search("年度", value)]
        # そのindexの後ろに、その要素+"_unit"の要素を入れる
        table_list[0].insert(value_index_list[0] + 1, table_list[0][value_index_list[0]] + "_unit")
        table_list[0].insert(value_index_list[1] + 2, table_list[0][value_index_list[1] + 1] + "_unit")
        result_df = pd.DataFrame(table_list[1:], columns=table_list[0])
    elif max(col_num_list) - len(table_list[0]) == 4:
        table_list[0].insert(2, table_list[0][1] + "_unit")
        table_list[0].append(table_list[0][-1] + "_unit")
        table_list[0].insert(1, "blank1")
        table_list[0].insert(4, "blank2")
        result_df = pd.DataFrame(table_list[1:], columns=table_list[0])

    return result_df

def modify_df_individual(df, ecode):
    """
    個別のDataFrameを修正する
    ある程度パターンがわかったら汎用的に書きたい
    ToDo: 2行パターンなどの処理で一部不要になったものがあるかもしれないので除外する
    """
    # カラム名の例外
    if ecode == "E04196":
        df.columns = ["account", "前連結会計年度(平成30年3月31日)", "前連結会計年度(平成30年3月31日)_unit"
            , "当連結会計年度(平成31年3月31日)" ,"当連結会計年度(平成31年3月31日)_unit"]
    elif ecode == "E00067":
        df.columns = ["account", "前連結会計年度(2018年3月31日)", "前連結会計年度(2018年3月31日)_unit"
            , "当連結会計年度(2019年3月31日)", "当連結会計年度(2019年3月31日)_unit"]
    elif ecode == "E26332":
        df.columns = ["account", "前連結会計年度(2018年3月31日)", "前連結会計年度(2018年3月31日)_unit"
            , "当連結会計年度(2019年3月31日)", "当連結会計年度(2019年3月31日)_unit"]
    elif ecode == "E03640":
        df.columns = ["account", "blank1", "前連結会計年度(平成30年3月31日)", "前連結会計年度(平成30年3月31日)_unit"
            , "blank2", "blank3", "当連結会計年度(平成31年3月31日)", "当連結会計年度(平成31年3月31日)_unit", "blank4"]
    elif ecode == "E04137":
        df.columns = ["account", "前連結会計年度(2018年3月31日)", "前連結会計年度(2018年3月31日)_unit"
            , "当連結会計年度(2019年3月31日)", "当連結会計年度(2019年3月31日)_unit"]
    elif ecode == "E05018":
        df.columns = ["account", "前連結会計年度(2018年3月31日)", "前連結会計年度(2018年3月31日)_unit"
            , "当連結会計年度(2019年3月31日)", "当連結会計年度(2019年3月31日)_unit"]
    elif ecode == "E02185":
        df.columns = ["account", "前連結会計年度(2018年3月31日)", "前連結会計年度(2018年3月31日)_unit"
            , "当連結会計年度(2019年3月31日)", "当連結会計年度(2019年3月31日)_unit"]
    elif ecode == "E05306":
        df.columns = ["account", "前連結会計年度(2018年3月31日)", "前連結会計年度(2018年3月31日)_unit"
            , "blank1", "当連結会計年度(2019年3月31日)", "当連結会計年度(2019年3月31日)_unit", "blank2"]
    # 年度の表記に括弧がないパターン
    elif ecode == "E02089":
        df.columns = ['account', 'blank1', '前連結会計年度(2018年3月31日)', '前連結会計年度(2018年3月31日)_unit'
            , 'blank2', '当連結会計年度(2019年3月31日)', '当連結会計年度(2019年3月31日)_unit']
    elif ecode == "E01135":
        df.columns = ["account", "前連結会計年度(2018年3月31日)", "当連結会計年度(2019年3月31日)"]
    # 年度がないパターン
    elif ecode == "E01364":
        df.columns = ["account", "前連結会計年度(blank)", "当連結会計年度(blank)"]
    # 値の列と単位の列が逆、単位自体は分かれていなかったので個別対応
    elif ecode in ["E04149", "E05017", "E02244", "E05121", "E02258"]:
        colname_replace = list(df.columns)
        colname_replace[1], colname_replace[2] = colname_replace[2], colname_replace[1]
        colname_replace[-1], colname_replace[-2] = colname_replace[-2], colname_replace[-1]
        df.columns = colname_replace
        df.iloc[:, 1] = df.iloc[:, 2].apply(lambda x: get_unit(x))
        df.iloc[:, -2] = df.iloc[:, -1].apply(lambda x: get_unit(x))
    # その他
    elif ecode == "E01859":
        df.columns = ['account', '前連結会計年度(2018年3月31日)_unit', '前連結会計年度(2018年3月31日)'
            , '当連結会計年度(2019年3月31日)_unit', '当連結会計年度(2019年3月31日)']
        df.drop(df.index[0], inplace=True)
        df["前連結会計年度(2018年3月31日)_unit"] == "千円"
        df["当連結会計年度(2019年3月31日)_unit"] == "千円"
    elif ecode == "E00774":
        df["前連結会計年度(2018年3月31日)"] = df["前連結会計年度(2018年3月31日)"].str.replace("\(", "")
        df["当連結会計年度(2019年3月31日)"] = df["当連結会計年度(2019年3月31日)"].str.replace("\(", "")
    elif ecode == "E02123":
        df.columns = ["account", "前連結会計年度(2018年3月31日)", "前連結会計年度(2018年3月31日)_unit"
            , "当連結会計年度(2019年3月31日)", "当連結会計年度(2019年3月31日)_unit"]

    return df

def get_unit(str):
    """
    値の文字列から単位を取得する
    末尾の)or）は単位ではないので除外
    マッチしないNoneTypeのときは空文字を返す
    """
    ptn = re.search("(?<=[0-9])[^0-9]+$", str)

    if ptn is None:
        return ""
    else:
        output = ptn.group(0).replace("〃", "").replace("\)", "").replace("）", "").strip()
        return output

# def get_value(str):
#     """
#     値の文字列から数値を取得する
#     マッチしないNoneTypeのときはnp.nanを返したいので、値はfloatで返す
#     """
#     ptn = re.search(".*[0-9]+(?=[^0-9]*$)", str)
#
#     if ptn is None:
#         return np.nan
#     else:
#         output = ptn.group(0).replace("△", "-").replace(",", "").replace("－", "")
#         output = re.sub("\s+", "", output)
#         return float(output)

def get_value(account, value_str):
    """
    値の文字列から数値を取得する
    マッチしないNoneTypeのときはnp.nanを返したいので、値はfloatで返す
    accountが()で囲われているときは、()の除外処理も行う
    """
    # 数値のパターン
    if value_str is None:
        ptn = None
    else:
        ptn = re.search(".*[0-9]+(?=[^0-9]*$)", value_str)

    # accountカラムが()で囲われているかどうか
    acc_bracket = re.search("^\(.+\)$", account)

    if ptn is None:
        return np.nan
    elif acc_bracket:
        output = ptn.group(0).replace("△", "-").replace(",", "").replace("－", "").replace("\(", "").replace("（", "")
    else:
        output = ptn.group(0).replace("△", "-").replace(",", "").replace("－", "")

    output = re.sub("\s+", "", output)
    return float(output)

def fill_unit(Series_unit):
    """
    指定した単位カラムに対して、単位が存在するときに次の要素が空文字だった場合は
    その単位で埋める処理を行う
    """
    # 単位のカラムをリストにする
    # 同上を示す不要な文字列は削除
    unit_list_tmp = list(Series_unit.str.replace("〃", "").str.replace("\(", "").str.replace("\)", "").str.replace("－", "").str.replace("―", "").str.strip())
    # 単位が存在するときに、次の要素が空文字だった場合はその単位で埋める
    for i in range(len(unit_list_tmp) - 1):
        if unit_list_tmp[i] == "":
            continue
        elif i == len(unit_list_tmp):
            continue
        elif unit_list_tmp[i + 1] == "":
            unit_list_tmp[i + 1] = unit_list_tmp[i]
        else:
            continue

    return pd.Series(unit_list_tmp)

def sep_unit(df, col):
    """
    指定したカラムから単位を抜き出して新しくカラムを作る
    指定したカラムにある単位は削除して数値に変換する
    """
    # 単位のカラムのカラム名
    col_unit = col.strip() + "_unit"

    # 出力用のdf
    df_output = df.copy()

    # 単位のカラムを作る
    df_output[col_unit] = df_output[col].apply(lambda x: get_unit(x))

    return df_output

def sep_period(df):
    """
    カラム名に期間の情報があるので、それを別カラムに分離する
    カラム名からは期間の情報を削除
    """
    # 出力用のdf
    df_output = df.copy()

    # カラム名から期間情報を取得してdatetimeオブジェクトに変更
    # 和暦混じっていることが発覚したので、一旦文字列のまま出しておく
    # 当期
    cur_period_tmp = [re.sub("(^.*年度|末)", "", i) for i in df.columns if re.search("^当.+(?<=年度).*\(.+\)$", i)][0]
    cur_period_str = re.search("(?<=^\().+(?=\)$)", cur_period_tmp.strip()).group()
    #cur_period = dt.strptime(cur_period_str, '%Y年%m月%d日')
    # 前期
    prev_period_tmp = [re.sub("(^.*年度|末)", "", i) for i in df.columns if re.search("^前.+(?<=年度).*\(.+\)$", i)][0]
    prev_period_str = re.search("(?<=^\().+(?=\)$)", prev_period_tmp.strip()).group()
    #prev_period = dt.strptime(prev_period_str, '%Y年%m月%d日')

    # カラムを追加
    df_output["cur_period"] = cur_period_str
    df_output["prev_period"] = prev_period_str

    # カラム名から期間情報を削除して、連結と単体の表記揺れを統一
    #col_list = [re.sub("末|\(.*\d+年\d+月\d+日\)", "", i) for i in df_output.columns]
    col_list = [i.replace(cur_period_tmp, "").replace(prev_period_tmp, "") for i in df_output.columns]
    col_list = [re.sub("^当.+(年度|末)", "cur_value", i) for i in col_list]
    col_list = [re.sub("^前.+(年度|末)", "prev_value", i) for i in col_list]
    df_output.columns = col_list

    return df_output

def wareki2seireki(warekiYear):
    """
    和暦を西暦に変換する関数
    とりあえず関数だけ実装
    """

    pattern = re.compile('^(明治|大正|昭和|平成|令和)([元0-9０-９]+)年$')
    matches = pattern.match(warekiYear)

    if matches:

        era_name = matches.group(1)
        year = matches.group(2)

        if year == '元':
            year = 1
        else:
            if sys.version_info < (3, 0):
                year = year.decode('utf-8')
            year = int(jaconv.z2h(year, digit=True))

        if era_name == '明治':
            year += 1867
        elif era_name == '大正':
            year += 1911
        elif era_name == '昭和':
            year += 1925
        elif era_name == '平成':
            year += 1988
        elif era_name == '令和':
            year += 2018

        return str(year) +'年'

    return null

def load_tax_effect_csv(file):
    """
    ファイルを読み込む関数
    空ファイルはNoneを返す
    parse_xbrl.pyで一度出力することで空文字がnanになっているはずなので、
    読み込んだ時に要素が全てnanの列は削除する
    """
    try:
        df = pd.read_csv(file, sep=",", encoding="utf-8")
    except pd.errors.EmptyDataError:
        return None
    for colname, item in df.iteritems():
        if all(item.isnull()):
            df.drop(colname, axis=1, inplace=True)
    return df
