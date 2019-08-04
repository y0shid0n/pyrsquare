# 関数定義
# あとでクラスにしたほうがいいかも

import re
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import jaconv
from datetime import datetime as dt
import sys

def get_ecode(filename):
    """
    ファイル名からedinet codeを取得する
    """
    ecode = re.search("(?<=_)E[0-9]+(?=-)", filename)
    return ecode.group(0)

def get_acc_standard(obj):
    """
    ファイルから会計基準を取得する
    """
    acc_standard = obj.get_data_by_context_ref("jpdei_cor:AccountingStandardsDEI", "FilingDateInstant").get_value()
    return acc_standard

def get_table(obj, key, context_ref):
    """
    ファイルから必要なtableを抜き出す
    """
    # keyとcontext_refから該当箇所を取得
    current_year_assets = obj.get_data_by_context_ref(key, context_ref)

    # 該当箇所がない場合はNoneを返す
    if current_year_assets is None:
        return None

    # htmlをパース
    soup = BeautifulSoup(current_year_assets.get_value(), 'html.parser')

    # テーブルを指定（たぶん1つめだけでよさげ）
    table = soup.findAll("table")[0]
    return table


def table_to_pd(table):
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

    # pandas.DataFrameに変換
    # 1行目が全て空なら削除
    if result_list[0].count("") == len(result_list[0]):
        result_list = result_list[1:]
    #return(result_list)

    # 単位のカラムが分かれてる場合とそうでない場合があるので分岐
    col_num_list = [len(i) for i in result_list]
    print(col_num_list)
    if col_num_list.count(max(col_num_list)) == len(col_num_list):
        # 単位が分かれていなければそのまま
        result_df = pd.DataFrame(result_list[1:], columns=result_list[0])
    else:
        # 単位のカラム名が分かれている場合はカラム名をつける
        # この辺はうまくいかない可能性がありそう（どこに空白列があるかがわからないので）
        if max(col_num_list) - len(result_list[0]) == 2:
            result_list[0].insert(-2, result_list[0][-3] + "_unit")
            result_list[0].append(result_list[0][-1] + "_unit")
            result_df = pd.DataFrame(result_list[1:], columns=result_list[0])
        elif max(col_num_list) - len(result_list[0]) == 4:
            result_list[0].insert(2, result_list[0][1] + "_unit")
            result_list[0].append(result_list[0][-1] + "_unit")
            result_list[0].insert(1, "blank1")
            result_list[0].insert(4, "blank2")
            result_df = pd.DataFrame(result_list[1:], columns=result_list[0])

    # カラム名の全角半角の揺れをなくす
    # カラム名の先頭に空白文字がある場合があるので削除
    result_df.columns = [jaconv.z2h(i.strip(), digit=True, ascii=True, kana=False) for i in result_df.columns]

    # 1列目のカラム名はaccountにして空白文字削除
    result_df.columns = ["account"] + list(result_df.columns)[1:]
    result_df["account"] = result_df["account"].str.strip()

    return result_df

def get_unit(str):
    """
    値の文字列から単位を取得する
    マッチしないNoneTypeのときは空文字を返す
    """
    ptn = re.search("(?<=[0-9])[^0-9]+$", str)

    if ptn is None:
        return ""
    else:
        output = ptn.group(0).replace("〃", "").strip()
        return output

def get_value(str):
    """
    値の文字列から数値を取得する
    マッチしないNoneTypeのときはnp.nanを返したいので、値はfloatで返す
    """
    ptn = re.search(".*[0-9]+(?=[^0-9]*$)", str)

    if ptn is None:
        return np.nan
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
    unit_list_tmp = list(Series_unit.str.replace("〃", "").str.strip())
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
    col_unit = col + "_unit"

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
    cur_period_str = re.search("(?<=^\().+(?=\)$)", cur_period_tmp).group()
    #cur_period = dt.strptime(cur_period_str, '%Y年%m月%d日')
    # 前期
    prev_period_tmp = [re.sub("(^.*年度|末)", "", i) for i in df.columns if re.search("^前.+(?<=年度).*\(.+\)$", i)][0]
    prev_period_str = re.search("(?<=^\().+(?=\)$)", prev_period_tmp).group()
    #prev_period = dt.strptime(prev_period_str, '%Y年%m月%d日')

    # カラムを追加
    df_output["cur_period"] = cur_period_str
    df_output["prev_period"] = prev_period_str

    # カラム名から期間情報を削除して、連結と単体の表記揺れを統一
    #col_list = [re.sub("末|\(.*\d+年\d+月\d+日\)", "", i) for i in df_output.columns]
    col_list = [i.replace(cur_period_tmp, "").replace(prev_period_tmp, "") for i in df_output.columns]
    col_list = [re.sub("^当.+年度", "cur_value", i) for i in col_list]
    col_list = [re.sub("^前.+年度", "prev_value", i) for i in col_list]
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
    parse_xbrl.pyで一度出力することで空文字がnanになっているはずなので、
    読み込んだ時に要素が全てnanの列は削除する
    """
    df = pd.read_csv(file, sep=",", encoding="utf-8")
    for colname, item in df.iteritems():
        if all(item.isnull()):
            df.drop(colname, axis=1, inplace=True)
    return df
