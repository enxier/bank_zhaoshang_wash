#-- coding:utf8 --
import pandas as pd
from ast import literal_eval
from pandas.io.excel import ExcelWriter
import os
import sys
import time
t = time.time()
#创建文件夹
def mkdir(path):
    folder = os.path.exists(path)

    if not folder:  # 判断是否存在文件夹如果不存在则创建为文件夹
        os.makedirs(path)  # makedirs 创建文件时如果路径不存在会创建这个路径
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "    来自创建路径模块的消息：成功创建路径：" + path)
    else:
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + "    来自创建路径模块的消息：指定路径”" + path + "“已经存在，跳过创建步骤")
数据源 = r'''C:\Users\Abluex\Desktop\反馈\20211119招商线下反馈\解压\20211119085412132846    28779093\20211119085412132846    28779093交易流水1.CSV
C:\Users\Abluex\Desktop\反馈\20211119招商线下反馈\解压\20211119085412132846    28779093\20211119085412132846    28779093交易流水2.CSV
C:\Users\Abluex\Desktop\反馈\20211119招商线下反馈\解压\20211119085427132846    27225276\20211119085427132846    27225276交易流水1.CSV
C:\Users\Abluex\Desktop\反馈\20211119招商线下反馈\解压\20211119085434132846    66541587\20211119085434132846    66541587交易流水1.CSV'''
files = literal_eval("['" + 数据源.replace('\n' , "' , '").replace('\\' , "/") + "']")
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    已读取文件列表')
##创建‘银行明细清洗’文件夹
mkdir_loc = files[0][0:files[0].find('解压')+3] + '银行明细清洗/'
mkdir(mkdir_loc)
columns0 = ['司法编号','序号','交易日期','交易时间','客户号','客户名称','交易卡号','交易流水','交易套号','交易机构','账户代码','交易方向(D:借|C:贷)','币种名称','交易金额','联机余额','摘要名称','文字摘要','交易渠道','冲补账标记','经办柜员','对方帐号','对方客户名称','对方帐号开户机构名称','对手公私标识','对手客户证件国别','对手客户证件类型','对手客户证件号码','对方开户机构号','对手开户机构国别代码','对方开户地区','对手方账户类别','代办人','网上交易IP地址','网银MAC地址','网银设备号','代办人证件国别','代办人证件类型','代办人证件号码','跨境交易标识','交易方式标识','ATM机具编号','账户类型','账户类别','客户序号','ATM所属机构编号','对方客户号','交易地区编码','交易代码','交易摘要','交易地区名称','交易机构编号','币种','渠道名称','证件号']
columns1 = ['司法编号','序号','交易日期','交易时间','客户号','客户名称','交易卡号','交易流水','交易套号','交易机构','账户代码','交易方向(D:借|C:贷)','币种名称','交易金额','联机余额','摘要名称','文字摘要','交易渠道','冲补账标记','经办柜员','对方帐号','对方客户名称','对方帐号开户机构名称','对手公私标识','对手客户证件国别','对手客户证件类型','对手客户证件号码','对方开户机构号','对手开户机构国别代码','对方开户地区','对手方账户类别','代办人','网上交易IP地址','网银MAC地址','网银设备号','代办人证件国别','代办人证件类型','代办人证件号码','跨境交易标识','交易方式标识','ATM机具编号','账户类型','账户类别','客户序号','ATM所属机构编号','对方客户号','交易地区编码','交易代码','交易摘要','交易地区名称','交易机构编号','币种','渠道名称','证件号','54']
columns = ['账户开户名称','交易卡号','交易账号','交易时间','收付标志','交易金额','交易余额','交易对手账卡号','对手户名','摘要说明','交易网点名称','交易是否成功','交易类型','备注','对手开户银行','交易流水号','IP地址']
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    已设定出入标准字段')
column = 0
record = 1
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    进入清洗流程......')
for file in files:
    filename_loc = file.find('交易流水') - 32
    filename = str(record)+"_"+file[filename_loc:].replace('/', '').replace('.csv', '').replace('.CSV', '')
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    -------当前清洗文件为：' + filename)
    #print(filename)
    record = record + 1
    df = pd.read_csv(file, dtype='str',encoding='GB18030')
    ###判断有无字段名
    if '司法编号' not in list(df.columns):
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    检测到分文件(无字段名)，添加字段名')
        df = pd.read_csv(file, dtype='str', encoding='GB18030', header=None)
        #print(df.columns)
        df.columns = columns1
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    字段名添加成功')

    df.rename(columns=lambda x: x.replace('\t', ''), inplace=True)      #删除列名中的\t
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    成功删除列名中的制表符')
    for column in columns0:#删除所有的\t
        df[column] = df[column].map(lambda x: str(x).lstrip('\t').rstrip('\t')).astype(str)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    成功删除全表的制表符')
    ####字段匹配
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    开始字段匹配')
    df['账户开户名称'] = df['客户名称']
    df['交易账号'] = df['交易卡号']
    df['交易时间'] = df['交易日期'] +' '+df['交易时间']
    df['收付标志'] = df['交易方向(D:借|C:贷)'].map(lambda x: str(x).replace('D:借','出').replace('C:贷','进')).astype(str)
    df['交易余额'] = df['联机余额']
    df['交易对手账卡号'] = df['对方帐号']
    df['对手户名'] = df['对方客户名称']
    df['摘要说明'] = df['文字摘要']
    df['交易网点名称'] = '招商银行'
    df['交易是否成功'] = 'NULL'
    df['交易类型'] = df['摘要名称']
    df['备注'] = '[线下调取账单]'
    df['对手开户银行'] = df['对方帐号开户机构名称']
    df['交易流水号'] = df['交易流水']
    df['IP地址'] = df['网上交易IP地址']
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    字段匹配完毕')
    #print(df)
    #print(df.index)
    #print(df.columns)
    df.to_excel(mkdir_loc+'/%s.xlsx' % filename, index=False, columns=columns)
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    清洗数据已完成')
####合并
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    进入合并步骤')
file = [mkdir_loc + '/' + i for i in
        os.listdir(mkdir_loc + '/')]
li = []
for i in file:
    li.append(pd.read_excel(i, dtype='str'))
mkdir_cut = files[0][0:files[0].find('解压')] + '分解/'
mkdir(mkdir_cut)
#print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    已创建分解文件夹mkdir_cut:'+mkdir_cut)
filename_loc_all = mkdir_loc + str(round(t * 1000000))+'-所有人合成.xlsx'
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    合并各个任务文件......')
writer = pd.ExcelWriter(filename_loc_all)
pd.concat(li).to_excel(writer, 'Sheet1', index=False)
writer.save()
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    合并完成，总文件路径为：' + filename_loc_all)
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    按照卡来分解明细文件......')
df = pd.read_excel(filename_loc_all, dtype='str')
df = df.drop_duplicates(subset=columns , keep='first')
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    完成全字段匹配去重')
df['账户开户名称']=df['账户开户名称'].astype(str)
df['交易卡号']=df['交易卡号'].astype(str)
df['record'] = df['账户开户名称']+df['交易卡号']
uni_value = df['record'].unique()
#columns = ['账户开户名称','交易卡号','交易账号','交易时间','收付标志','交易金额','交易余额','交易对手账卡号','对手户名','摘要说明','交易网点名称','交易是否成功','交易类型','备注','对手开户银行','IP地址']
df=df.astype(str)
#df = df.where(df.notnull(), None)
for column in columns:  # 删除所有的nan
    df[column] = df[column].map(lambda x: str(x).lstrip('nan').rstrip('nan')).astype(str)
i = 1
for s in uni_value:
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    成功分解:  ' + s)
    filename = s.replace('\t','')
    data_s = df[df['record'] == s]
    # 保存拆分后的工作簿到文件夹中
    data_s.to_excel(mkdir_cut + '%s-招商.xlsx' % filename, index=False, columns=columns)
    i = i + 1
print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '    任务完成，共清洗出' + str(i) + '张线下调取的招商银行卡')