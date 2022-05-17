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
        d = root.find('.//' + chk + 'Date')  # get date from cheque
        date = d.text

        with open(filename) as f:  # create base64 from cheque
            chq = f.read()
            ch_bytes = chq.encode('utf-8')
            ch_b64_bytes = base64.b64encode(ch_bytes)
            ch_b64_string = ch_b64_bytes.decode()

        cheque_info = {
            "date": date,
            "data": ch_b64_string
        }
        return cheque_info

    def get_chq_attrib(self, filename, root):
        return self.__get_chq_attrib__(filename, root)


class ParserChequeV0:

    @classmethod
    def __get_chq_attrib__(cls, filename, root):  # grep date from cheq
        date_year = '20' + root.attrib['datetime'][4:6]
        date_month = root.attrib['datetime'][2:4]
        date_day = root.attrib['datetime'][:2]
        date_time = 'T' + root.attrib['datetime'][6:8] + ':' + root.attrib['datetime'][-2:] + ':00'
        date = date_year + '-' + date_month + '-' + date_day + date_time

        with open(filename, 'rb') as f:  # make base64 from chq
            chq_bin = f.read()
            ch_b64_bytes = base64.b64encode(chq_bin)
            ch_b64_string = ch_b64_bytes.decode()

        info_cheque = {  # json for data from cheque
            "date": date,
            "data": ch_b64_string
        }

        return info_cheque

    def get_chq_attrib(self, filename, root):
        return self.__get_chq_attrib__(filename, root)


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
                srv = 'gitlab-ci.ru:9092'
                co = pyodbc.connect(driver + ms)
                cursor = co.cursor()
                return cursor, srv, database, co
            elif contur == 1:
                server = '10.10.4.7'
                database = 'WBfileStoreCheque'
                username = 'fileviewer'
                password = 'qwe123'
                ms = f'PORT=1433;SERVER={server}; DATABASE={database}; UID={username}; PWD={password}'
                srv = '10.10.4.28:9092'
                co = pyodbc.connect(driver + ms)
                cursor = co.cursor()
                return cursor, srv, database, co
            elif contur == 5:
                server = '10.10.4.173'
                database = 'UnionChequeTWB'
                username = 'fileviewer'
                password = 'qwe123'
                ms = f'PORT=1433;SERVER={server}; DATABASE={database}; UID={username}; PWD={password}'
                srv = 'prod-kafka-01.nd.fsrar.ru:9092'
                co = pyodbc.connect(driver + ms)
                cursor = co.cursor()
                return cursor, srv, database, co

        self.cursor, self.srv, self.database, self.co = _set_contur(contur)

    def fetch_from_db(self, ui_date=None):
        strip_date = ui_date.replace("-", "")
        exe = f"""SELECT OwnerId, TransportId, fileData FROM {self.database}.dbo.Files WITH (NOLOCK)
        WHERE (insertDate BETWEEN '{strip_date} 00:00:00.000' AND '{strip_date} 23:59:59.000') AND  
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

    def __makejson_inbox__(self, filepath, fsrar_id, transport_id):
        def check_docktype(root):
            if 'ChequeV3' in str(root):
                type = 'ChequeV3'
                ch = ParserChequeV3()
                attr = ch.get_chq_attrib(filepath, root)
                return type, attr
            else:
                type = 'Cheque'
                chutm = ParserChequeV0()
                attr = chutm.get_chq_attrib(filepath, root)
                return type, attr

        try:  # try parse from file
            print("parse from file")
            tree = xml.parse(filepath)
            root = tree.getroot()

        except xml.ParseError as e:  # if scr can't parse from file: read file -> save to string -> parse from string
            print(e)
            print("read file and parse from string")
            with open(filepath) as f:
                file = f.read()
            root = xml.fromstring(file)

        type, attr = check_docktype(root)
        certificate = ''
        sign = ''
        data = attr['data']
        date = attr['date'][:10]
        uri = fsrar_id + '-' + transport_id
        json1 = {
            'certificate': certificate,
            'sign': sign,
            'type': type,
            'data': data,
            'date': date,
            'fsrarid': fsrar_id,
            'uri': uri
        }
        jsonString = json.dumps(json1)
        return jsonString


def main():
    x = xmltojson_inbox_base()
    print("value in () is default")
    date = input('date (2022-02-16) \n > ')
    contur = input("0-sand \n1-test \n(5-prod) \n > ")

    get_con = GetFilesFromDB(0 if len(contur) == 0 else int(contur))  # 0-sand 1-test 5-prod
    row_list = get_con.fetch_from_db('2022-02-16' if len(date) == 0 else date)
    cnt_checks = len(row_list)
    print(cnt_checks, ' для выбранного часа')

    st = 1 + int(input(' start from "idx" > '))
    # st = 0

    for idx, row in enumerate(row_list[st:]):

        print("-"*50)
        file = row["fileData"]

        filepath = PATH + FILENAME  # file path to save file near script
        with open(filepath, 'w') as f:
            f.write(file.decode('utf-8'))

        fsrarid = row['OwnerId']
        transport_id = row['TransportId']
        jsonString = x.__makejson_inbox__(filepath=filepath, fsrar_id=fsrarid, transport_id=transport_id)
        get_con.send_json(jsonString, x)
        # print(jsonString)
        print(fsrarid, transport_id, f'{idx + st + 1} / {cnt_checks}', transport_id[:8])


if __name__ == '__main__':
    main()
