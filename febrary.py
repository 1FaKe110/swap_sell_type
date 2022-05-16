import base64
import json
import xml.etree.ElementTree as xml
import random as ra
import pyodbc
import confluent_kafka as ck

FILENAME = 'file'
PATH = '/home/ldapusers/gabko/python_scr/'


class ParserChequeV3:

    @classmethod
    def __get_chq_attrib__(cls, filename, root):

        chk = '{http://fsrar.ru/WEGAIS/ChequeV3}'
        d = root.find('.//' + chk + 'Date')
        _date = d.text

        with open(filename) as f:
            chq = f.read()
            ch_bytes = chq.encode('utf-8')
            ch_b64_bytes = base64.b64encode(ch_bytes)
            ch_b64_string = ch_b64_bytes.decode()

        json1 = {"date": _date,
                 "data": ch_b64_string}
        return json1

    def get_chq_attrib(self, filename, root):
        return self.__get_chq_attrib__(filename, root)


class chequeutm():
    def __chequeutm__(self, filename, root):
        date_year = '20' + root.attrib['datetime'][4:6]
        date_month = root.attrib['datetime'][2:4]
        date_day = root.attrib['datetime'][:2]
        date_time = 'T' + root.attrib['datetime'][6:8] + ':' + root.attrib['datetime'][-2:] + ':00'
        _date = date_year + '-' + date_month + '-' + date_day + date_time

        with open(filename) as ch:
            cheque = ch.read()
            ch_bytes = cheque.encode('utf-8')
            ch_b64_bytes = base64.b64encode(ch_bytes)
            ch_b64_string = ch_b64_bytes.decode()

        json1 = {"date": _date,
                 "data": ch_b64_string}

        return json1

    def get_chequeutm_attr(self, filename, root):
        return self.__chequeutm__(filename, root)


class GetFilesFromDB:
    def __init__(self, contur):
        self.cur_reply = None

        def _set_contur(contur):
            driver = 'DRIVER={ODBC Driver 17 for SQL Server};'
            if contur == 0:
                server = '31.31.199.107'
                database = 'UnionChequeTWB'
                username = 'zaglushki_read'
                password = 'nEb*hd17QK9dj*2'
                ms = f'PORT=1433;SERVER={server}; DATABASE={database}; UID={username}; PWD={password}'
                co = pyodbc.connect(driver + ms)
                cursor = co.cursor()
                srv = 'gitlab-ci.ru:9092'
                return cursor, srv, database
            elif contur == 1:
                server = '10.10.4.7'
                database = 'WBfileStoreCheque'
                username = 'fileviewer'
                password = 'qwe123'
                ms = f'PORT=1433;SERVER={server}; DATABASE={database}; UID={username}; PWD={password}'
                co = pyodbc.connect(driver + ms)
                cursor = co.cursor()
                srv = '10.10.4.28:9092'
                return cursor, srv, database
            elif contur == 5:
                server = '10.10.4.173'
                database = 'UnionChequeTWB'
                username = 'fileviewer'
                password = 'qwe123'
                ms = f'PORT=1433;SERVER={server}; DATABASE={database}; UID={username}; PWD={password}'
                co = pyodbc.connect(driver + ms)
                cursor = co.cursor()
                srv = 'prod-kafka-01.nd.fsrar.ru:9092'
                return cursor, srv, database, co

        self.cursor, self.srv, self.database, self.co = _set_contur(contur)

    def fetch_from_db(self, date=""):
        date = date.replace("-", "")
        exe = f"""SELECT OwnerId, TransportId, fileData FROM {self.database}.dbo.Files WITH (NOLOCK)
        WHERE (insertDate BETWEEN '{date} 11:00:00.000' AND '{date} 11:59:59.000') AND  
        (DocType = 'Cheque' or DocType = 'ChequeV3');
        """
        cur_reply = self.cursor.execute(exe)
        columns = [column[0] for column in cur_reply.description]
        print(columns)
        results = []
        for row in cur_reply.fetchall():
            results.append(dict(zip(columns, row)))
        self.cursor.close()
        self.co.close()
        return results

    def send_json(self, jsonString, x):
        proconf = {'bootstrap.servers': self.srv, 'client.id': 'gabko'}
        topic = 'inbox'
        producer = ck.Producer(proconf)
        producer.produce(topic, key=x.__keyhold__(), value=jsonString)
        producer.flush()
        # print("send json to inbox")


class xmltojson_inbox_base():
    def __keyhold__(self):
        t = 10000
        aint = ra.randint(t, t * t)
        return aint.to_bytes(4, byteorder='big')

    def __makejson_inbox__(self, filename, fsrar_id, transport_id):
        def check_docktype(root):
            if 'ChequeV3' in str(root):
                type = 'ChequeV3'
                ch = ParserChequeV3()
                attr = ch.get_chq_attrib(filename, root)
                return type, attr
            else:
                type = 'Cheque'
                chutm = chequeutm()
                attr = chutm.get_chequeutm_attr(filename, root)
                return type, attr

        tree = xml.parse(filename)
        root = tree.getroot()

        type, attr = check_docktype(root)
        certificate = ''
        sign = ''
        data = attr[0]['data']
        date = attr[0]['date'][:10]
        fsrarid = fsrar_id
        uri = fsrarid + '-' + transport_id
        json1 = {
            'certificate': certificate,
            'sign': sign,
            'type': type,
            'data': data,
            'date': date,
            'fsrarid': fsrarid,
            'uri': uri
        }
        jsonString = json.dumps(json1)
        return jsonString


x = xmltojson_inbox_base()
print("value in () is default")
date = input('date (2022-02-19) \n > ')
contur = input("0-sand \n1-test \n(5-prod) \n > ")
get_con = GetFilesFromDB(5 if len(contur) == 0 else int(contur))  # 0-sand 1-test 5-prod
row_list = get_con.fetch_from_db('2022-02-19' if len(date) == 0 else date)
cnt_checks = len(row_list)
print(cnt_checks, ' для выбранного часа')

st = 1 + int(input(' start from "idx" > '))
# st = 0

for idx, row in enumerate(row_list[st:]):
    file = row["fileData"]

    filepath = PATH + FILENAME
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(file.decode('utf-8'))

    fsrarid = row['OwnerId']
    transport_id = row['TransportId']
    jsonString = x.__makejson_inbox__(filename=filepath, fsrar_id=fsrarid, transport_id=transport_id)
    get_con.send_json(jsonString, x)
    # print(jsonString)
    print(fsrarid, transport_id, f'{idx + st} / {cnt_checks}', transport_id[:8])
