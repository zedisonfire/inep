from selenium import webdriver
import os
from datetime import datetime
import pandas as pd
import geopandas.tools
from shutil import copyfile
from selenium.webdriver.chrome.options import Options
from time import sleep
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import fileinput
import numpy as np

tokens = 'D:/INEP/CPF'


def yearbysubscription():
    return {'2009': '2009',
            '2010': '2010',
            '1110': '2011',
            '1120': '2011',
            '1201': '2012',
            '1202': '2012',
            '1310': '2013',
            '1320': '2013',
            '1410': '2014',
            '1420': '2014',
            '1510': '2015',
            '1520': '2015',
            '1610': '2016',
            '1620': '2016'}


def getenemcolumns():
    return ['Número de Inscrição', 'CPF', 'Nome', 'Nota CN', 'Nota CH', 'Nota LC',
            'Nota MT', 'Nota RD', 'Presença CN', 'Presença CH', 'Presença LC',
            'Presença MT', 'Presença RD', 'Data de Nascimento', 'Sexo', 'RG',
            'Orgão Expeditor', 'UF Expedição', 'UF Local de Prova', 'Município Local de Prova',
            'Unidade Prisional', 'Língua Extrangeira']


def validatepresences(df, char, result):
    presences = ['Presença CN', 'Presença CH', 'Presença LC', 'Presença MT', 'Presença RD']

    for presence in presences:
        df.loc[df[presence] == char, presence] = result
        df.loc[df[presence].isnull(), presence] = 6


def finalmerge():
    columns = ['token', 'arquivo', 'Ano']
    df_log = pd.read_csv('D:/inep.log', sep='|', decimal=',', encoding='ISO-8859-1', low_memory=False, error_bad_lines=False)

    years = pd.DataFrame(columns=columns)
    for i in range(2, 10):
        year = getyear()[i]
        df = df_log[['token', 'response file '+year, 'download '+year]].dropna(axis=0, how='all')
        df['download '+year] = year
        df.columns = columns
        years = years.append(df, ignore_index=True)

    years = years.dropna(thresh=2)
    df_a = pd.DataFrame(columns=['CPF', 'Ano'])
    for index, col in years.iterrows():
        df_tmp = pd.read_csv('D:/INEP/CPF/'+col['token'], sep='|', decimal=',', encoding='ISO-8859-1', low_memory=False, error_bad_lines=False)

        df_tmp.columns = ['CPF']
        df_tmp['Ano'] = col['Ano']
        df_a = df_a.append(df_tmp, ignore_index=True)

    df_b = pd.DataFrame(columns=getenemcolumns())
    columns_len = len(getenemcolumns())
    for index, col in years.iterrows():
        if col['arquivo'] == '' or str(col['arquivo']) == 'nan':
            continue
        df_tmp = pd.read_csv('D:/INEP/Downloaded/'+str(col['arquivo']), sep=';', decimal='.', encoding='ISO-8859-1', low_memory=False, error_bad_lines=False)
        df_tmp_len = len(df_tmp.columns)
        if df_tmp_len != columns_len:
            if df_tmp_len > columns_len:
                df_tmp = df_tmp.drop(df_tmp.columns[columns_len], axis=1)
            else:
                i = df_tmp_len
                while len(df_tmp.columns) < columns_len:
                    df_tmp[i] = ''
                    i += 1

        df_tmp.columns = getenemcolumns()
        df_tmp['Ano'] = col['Ano']
        df_b = df_b.append(df_tmp, ignore_index=True)

    df_final = pd.merge(df_a, df_b, on=['CPF', 'Ano'], how='outer')

    df_final = df_final[['Ano', 'Número de Inscrição', 'CPF', 'Nome', 'Nota CN', 'Nota CH', 'Nota LC',
            'Nota MT', 'Nota RD', 'Presença CN', 'Presença CH', 'Presença LC',
            'Presença MT', 'Presença RD', 'Data de Nascimento', 'Sexo', 'RG',
            'Orgão Expeditor', 'UF Expedição', 'UF Local de Prova', 'Município Local de Prova',
            'Unidade Prisional', 'Língua Extrangeira']]

    df_final['Status'] = df_final['Nota CN'] * df_final['Nota CH'] * df_final['Nota LC'] * df_final['Nota MT'] * df_final['Nota RD']

    df_final.loc[df_final.Status.isnull(), 'Status'] = -1
    df_final.loc[df_final.Status > 0, 'Status'] = 1

    chars = [{0: 7, 1: 'P'}, {0: 6, 1: 'F'}, {0: 6, 1: '-'}, {0: 6, 1: ''}, {0: 6, 1: ' '}, {0: 2, 1: 'B'}, {0: 2, 1: 'N'}, {0: 2, 1: 'D'}, {0: 5, 1: 'T'}]

    for char in chars:
        validatepresences(df_final, char[1], char[0])

    pd.DataFrame.to_csv(df_final, 'D:/INEP/final.txt', sep='|', decimal=',', encoding='ISO-8859-1', header=True, index=False)


def merge():
    columns = getenemcolumns()
    df = pd.DataFrame(columns=columns)

    for f in os.listdir('D:/INEP/Downloaded'):
        df_tmp = pd.read_csv('D:/INEP/Downloaded/' + f, sep=';', decimal='.', encoding='ISO-8859-1',
                             low_memory=False, error_bad_lines=False)

        i = len(df_tmp.columns)
        while len(df_tmp.columns) < len(columns):
            df_tmp[i] = ''
            i += 1

        if 'Unnamed: 22' in df_tmp.columns:
            df_tmp.__delitem__('Unnamed: 22')

        df_tmp.columns = columns

        df = df.append(df_tmp, ignore_index=True)
    year = []
    for index, rows in df.iterrows():
        year.append(yearbysubscription()[str(rows['Número de Inscrição'])[0:4]])

    df['Ano'] = year
    df.fillna(0)
    pd.DataFrame.to_csv(df, 'D:/INEP/merge.txt', sep='|', decimal=',', encoding='ISO-8859-1', header=True, index=False)


def getdriver():
    options = Options()
    options.add_experimental_option(
            "prefs", {
                "download.default_directory": r"D:/INEP/Downloaded",
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
        })

    return webdriver.Chrome('C:\Python36\chromedriver.exe', chrome_options=options)


def getcount():
    return {'2016': {0: "1",  1: '2'},
            '2015': {0: "3",  1: '4'},
            '2014': {0: "5",  1: '6'},
            '2013': {0: "7",  1: '8'},
            '2012': {0: "9",  1: '10'},
            '2011': {0: "11", 1: '12'},
            '2010': {0: "13", 1: '14'},
            '2009': {0: "15", 1: '16'}}


def getyear():
    return {2: '2016',
            3: '2015',
            4: '2014',
            5: '2013',
            6: '2012',
            7: '2011',
            8: '2010',
            9: '2009'}


def getlogcolumns():
    return ['upload 2009', 'download 2009', 'response file 2009',
            'upload 2010', 'download 2010', 'response file 2010',
            'upload 2011', 'download 2011', 'response file 2011',
            'upload 2012', 'download 2012', 'response file 2012',
            'upload 2013', 'download 2013', 'response file 2013',
            'upload 2014', 'download 2014', 'response file 2014',
            'upload 2015', 'download 2015', 'response file 2015',
            'upload 2016', 'download 2016', 'response file 2016']


def openpage(driver):
    driver.get('http://sistemasenem.inep.gov.br/EnemSolicitacao/')
    sleep(5)
    #print(len(driver.page_source))
    if len(driver.page_source) != 337:
        driver.close()
        openpage(driver)


def login(driver):
    driver.switch_to.frame(driver.find_elements_by_tag_name('iframe')[0])

    access = open('C:/Python36/access.txt', 'r').readlines()

    elem = driver.find_element_by_id('username')
    elem.clear()
    elem.send_keys(access[0])
    elem = driver.find_element_by_id('password')
    elem.clear()
    elem.send_keys(access[1])


def uploadanddownload(driver):
    for i in range(2, 10):
        log(getyear()[i], driver, i)


def gettmp():
    return 'D:/tmp'


def tmpfiles(token, year):
    try:
        tmp = gettmp()
        if not os.path.exists(tmp):
            os.makedirs(tmp)

        copyfile(os.path.join(tokens + '/' + token), os.path.join(tmp,  year + '_' + token))
        return os.path.join(tmp,  year + '_' + token)
    except:
        return False


def upload(token, year, driver, i):
    driver.find_element_by_xpath(".//*[@id='menuForm']/ul[2]/li[" + str(i) + "]/ul/li[3]/a").click()
    sleep(2)
    tmp = tmpfiles(token, year)

    if not tmp:
        return False

    try:
        driver.find_element_by_id('uploadid:file').send_keys(tmp)
        driver.find_element_by_id('uploadid:upload2').click()
        for element in driver.find_element_by_xpath(".//*[@id='uploadid:fileItems']/table/tbody/tr/td[1]/div[3]"):
            if element.text != 'Transferido com sucesso':
                upload(token, year, driver, i)
            os.unlink(tmp)
        #driver.find_element_by_xpath('//div/input[@src="http://public.inep.gov.br/MECdefault/files/images/botoes/acao/vermelho/voltar.jpg"]').click()
    except:
        return False


def gotodownload(driver):
    for i in range(9, 1, -1):
        driver.find_element_by_link_text('Acompanhar solicitação').click()
        sleep(3)
        log(getyear()[i], driver)


def download(token, year, driver, rows):
    #if rows > 200:
        #return False
    today = datetime.strftime(datetime.now(), '%d/%m/%Y')
    count = rows
    for element in driver.find_elements_by_xpath(".// *[ @ id = 'listaSolicitacaoNaoAtendidas:tb']"):
        if year + '_' + token in element.text:
            for element in driver.find_elements_by_xpath('html/body/div[2]/div[3]/form/div[2]/div/span/table/tbody/tr'):
                if year + '_' + token in element.text and today in element.text:
                    return 'REJECT'
                count += 1

    count = rows
    for element in driver.find_elements_by_xpath(".// *[ @ id = 'listaSolicitacaoAtendidas:tb']"):
        if year + '_' + token in element.text:
            for element in driver.find_elements_by_xpath('html/body/div[2]/div[3]/form/div[3]/div/span/table/tbody/tr'):
                if year + '_' + token in element.text and today in element.text:
                    driver.find_element_by_xpath(".//*[@id='listaSolicitacaoAtendidas:"+str(count)+":j_id248']/a").click()
                    #driver.find_element_by_link_text('Acompanhar solicitação').click()
                    sleep(6)
                    return True
                count += 1

    return False
    #sleep(5)
    #driver.find_element_by_xpath('// *[ @ id = "listaSolicitacaoAtendidasDataScroller_table"] / tbody / tr / td[10]').click()
    #rows += 5
    #sleep(3)
    #download(token, year, driver, rows)


def newtoken(token):
    df = pd.DataFrame(index=pd.Index(os.listdir(tokens)))
    df.set_index(token)


def log(year, driver, i):
    df = pd.read_csv('D:/inep.log', sep='|', decimal=',', encoding='ISO-8859-1', low_memory=False, error_bad_lines=False,keep_default_na=False)
    #action = fn + ' ' + year
    fn = ['upload', 'download']
    action = [fn[0] + ' ' + year, fn[1] + ' ' + year]
    today = datetime.strftime(datetime.now(), '%d/%m/%Y')
    for index, row in df.iterrows():
        if row[action[0]] == '':
            globals()[fn[0]](row['token'], year, driver, i)
            sleep(3)
            df.at[index, action[0]] = today
            pd.DataFrame.to_csv(df, 'D:/inep.log', sep='|', decimal=',', encoding='ISO-8859-1', header=True,
                                    index=False)

        if row[action[1]] == '':
            if row[action[0]] == '':
                log(year, driver, i)
            sleep(10)
            before = os.listdir('D:/INEP/Downloaded')
            driver.find_element_by_link_text('Acompanhar solicitação').click()
            if globals()[fn[1]](row['token'], year, driver, 0) == 'REJECT':
                df.at[index, action[1]] = 'REJECT'
            else:
                after = os.listdir('D:/INEP/Downloaded')
                change = set(after) - set(before)
                if len(change) == 1:
                    response = change.pop()
                    df.at[index, action[1]] = today
                    df.at[index, 'response file '+year] = response

            pd.DataFrame.to_csv(df, 'D:/inep.log', sep='|', decimal=',', encoding='ISO-8859-1', header=True, index=False)


def delete(token):
    df = pd.read_csv('D:/inep.log', sep='|', decimal=',', encoding='ISO-8859-1', low_memory=False, error_bad_lines=False)
    for index, row in df.iterrows():
        if row['token'] == token:
            df.drop(index)
            return True

    return False


def createlog():
    if os.path.exists('D:/inep.log'):
        return

    df = pd.DataFrame(index=pd.Index(os.listdir(tokens)))

    for i in getlogcolumns():
        df[i] = ''

    df.fillna(0)
    pd.DataFrame.to_csv(df, 'D:/inep.log', sep='|', decimal=',', encoding='ISO-8859-1', header=True, index=True, index_label='token')


def incrementlog():
    df = pd.read_csv('D:/inep.log', sep='|', decimal=',', encoding='ISO-8859-1', low_memory=False, error_bad_lines=False, keep_default_na=False)

    for token in os.listdir('D:\INEP\CPF'):
            log = True
            for index, row in df.iterrows():
                if token == row['token']:
                    log = False

            if log:
                newtoken = pd.Series([token, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''], index=df.columns)

                df = df.append(newtoken, ignore_index=True)
                pd.DataFrame.to_csv(df, 'D:/inep.log', sep='|', decimal=',', encoding='ISO-8859-1', header=True, index=False)


def getrejectstatus(status):
    if status == '':
        return 'REJECT'
    elif status == 'REJECT':
        return status+' 2'
    return 'NO RESPONSE'


def validaterejects(driver):
    df = pd.read_csv('D:/inep.log', sep='|', decimal=',', encoding='ISO-8859-1', low_memory=False,
                     error_bad_lines=False, keep_default_na=False)
    for i in range(2, 10):
        rejects = ['REJECT', 'REJECT 2']
        fn = ['upload', 'download', getyear()[i]]
        year = getyear()[i]
        today = datetime.strftime(datetime.now(), '%d/%m/%Y')
        for index, row in df.iterrows():
            if row[fn[1] + ' ' + year] in rejects:
                globals()[fn[0]](row['token'], year, driver, i)
                sleep(3)
                df.at[index, fn[0] + ' ' + year] = today
                pd.DataFrame.to_csv(df, 'D:/inep.log', sep='|', decimal=',', encoding='ISO-8859-1', header=True,
                                    index=False)

                sleep(10)
                before = os.listdir('D:/INEP/Downloaded')
                driver.find_element_by_link_text('Acompanhar solicitação').click()
                if globals()[fn[1]](row['token'], getyear()[i], driver, 0) == rejects[0]:
                    df.at[index, fn[1] + ' ' + year] = getrejectstatus(row[fn[1] + ' ' + year])
                else:
                    after = os.listdir('D:/INEP/Downloaded')
                    change = set(after) - set(before)
                    if len(change) == 1:
                        response = change.pop()
                        df.at[index, fn[1] + ' ' + year] = today
                        df.at[index, 'response file ' + year] = response

                pd.DataFrame.to_csv(df, 'D:/inep.log', sep='|', decimal=',', encoding='ISO-8859-1', header=True,
                                    index=False)
            if row[fn[1] + ' ' + year] in rejects:
                validaterejects(driver)
''

def exec(driver):
    try:
        createlog()
        incrementlog()
        openpage(driver)
        login(driver)
        uploadanddownload(driver)
        sleep(10)
        validaterejects(driver)
        sleep(10)
        driver.close()
        merge()
        finalmerge()
        'a'
        return True
    except Exception as ex:
            return ex


driver = getdriver()
if exec(driver) is not True:
    driver.close()
    exec(driver)
