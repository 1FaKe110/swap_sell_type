import base64
import json
import xml.etree.ElementTree as xml
import random as ra
import pyodbc
import confluent_kafka as ck


class cheque_v3_header:
    def __cheque_v3_header__(self, root, transport_id):
        ns = '{http://fsrar.ru/WEGAIS/WB_DOC_SINGLE_01}'
        chk = '{http://fsrar.ru/WEGAIS/ChequeV3}'
        doctype = 'chequeV3'
        uid = transport_id
        transportid = uid.replace('-', '')
        d = root.find('.//' + chk + 'Date')
        date = d.text
        f = root.find('.//' + ns + 'FSRAR_ID')
        fsrarid = f.text
        t = root.find('.//' + chk + 'Type')
        t1 = t.text
        if t1 == 'Продажа':
            return1 = '0'
        else:
            return1 = '1'
        k = root.find('.//' + chk + 'Kassa')
        kassa = k.text
        sh = root.find('.//' + chk + 'Shift')
        shift = sh.text
        n = root.find('.//' + chk + 'Number')
        number = n.text

        filename = 'f'
        x = xml.tostring(root, encoding="unicode", method="xml")
        with open(filename, 'w', encoding="utf-8") as f:
            f.write(x)

        with open(filename) as ch:
            cheque = ch.read()
            ch_bytes = cheque.encode('utf-8')
            ch_b64_bytes = base64.b64encode(ch_bytes)
            ch_b64_string = ch_b64_bytes.decode()
        bottleattrs = {}
        bottles = []
        for i in root.findall('.//' + chk + 'Bottle'):
            for tag in i:
                key = str(tag.tag[33:]).lower()
                bottleattrs[key] = tag.text
            if bottleattrs['ean'] is None:
                bottleattrs['ean'] = ''
            bottle = bottleattrs
            bottles.append(bottle)
            bottleattrs = {}
        json1 = {"doctype": doctype, "fsrarid": fsrarid, "transportid": transportid, "return": return1, "date": date,
                 "kassa": kassa, "shift": shift, "number": number, "content": {"position": bottles},
                 "data": ch_b64_string}
        return json1, bottles

    def get_ch_v3_header_attr(self, root, transport_id):
        return self.__cheque_v3_header__(root, transport_id)


class chequeutm():
    def __chequeutm__(self, root, transport_id, fsrar):
        doctype = 'cheque'
        uid = transport_id
        transportid = uid.replace('-', '')
        kassa = root.attrib['kassa']
        shift = root.attrib['shift']
        number = root.attrib['number']
        date_year = '20' + root.attrib['datetime'][4:6]
        date_month = root.attrib['datetime'][2:4]
        date_day = root.attrib['datetime'][:2]
        date_time = 'T' + root.attrib['datetime'][6:8] + ':' + root.attrib['datetime'][-2:] + ':00'
        date = date_year + '-' + date_month + '-' + date_day + date_time
        fsrarid = fsrar
        bottles = []
        for i in root.findall('Bottle'):
            bottle = i.attrib
            if 'volume' in bottle:
                del bottle['volume']
            if '-' in str(bottle['price']):
                return1 = '1'
                bottle['price'] = bottle['price'][1:]
            else:
                return1 = '0'
            bottles.append(bottle)

        filename = 'f'
        x = xml.tostring(root, encoding="unicode", method="xml")
        with open(filename, 'w', encoding="utf-8") as f:
            f.write(x)

        with open(filename) as ch:
            cheque = ch.read()
            ch_bytes = cheque.encode('utf-8')
            ch_b64_bytes = base64.b64encode(ch_bytes)
            ch_b64_string = ch_b64_bytes.decode()
        json1 = {"doctype": doctype, "fsrarid": fsrarid, "transportid": transportid, "return": return1, "date": date,
                 "kassa": kassa, "shift": shift, "number": number, "content": {"position": bottles},
                 "data": ch_b64_string}
        return json1, bottles

    def get_chequeutm_attr(self, root, transport_id, fsrar):
        return self.__chequeutm__(root, transport_id, fsrar)


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
        WHERE (insertDate BETWEEN '{date} 00:00:00.000' AND '{date} 23:59:59.000') AND  
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

    def __makejson_inbox__(self, file, fsrarid, uri):
        def check_docktype(root):
            if 'ChequeV3' in str():
                type = 'ChequeV3'
                ch = cheque_v3_header()
                attr = ch.get_ch_v3_header_attr(root, transport_id=uri)
                return type, attr
            else:
                type = 'Cheque'
                chutm = chequeutm()
                attr = chutm.get_chequeutm_attr(root, transport_id=uri, fsrar=fsrarid)
                return type, attr

        try:
            root = xml.fromstring(file)
        except Exception as e:
            print(e)
            print("can't parse string, trying save to file and parse it")
            filename = 'file'
            with open(filename, 'w', encoding='windows-1251') as f:
                f.write(file.decode('utf-8'))
            tree = xml.parse(filename)
            root = tree.getroot()

        type, attr = check_docktype(root)
        certificate = ''
        sign = ''
        data = attr[0]['data']
        date = attr[0]['date'][:10]
        fsrarid = fsrarid
        uri = fsrarid + '-' + uri
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
print('поменяй срез списка!!!!!!')
print("value in () is default")
date = input('date (2022-02-19) \n > ')
contur = input("0-sand \n1-test \n(5-prod) \n > ")
get_con = GetFilesFromDB(5 if len(contur) == 0 else int(contur))  # 0-sand 1-test 5-prod
row_list = get_con.fetch_from_db('2022-02-19' if len(date) == 0 else date)
cnt_checks = len(row_list)
print(cnt_checks, ' для выбранного часа')

st = 1 + int(input(' start from "idx" > '))
# st = 0

for idx, row in enumerate(row_list[st:st + 1]):
    file = row["fileData"]
    fsrarid = row['OwnerId']
    uri = row['TransportId']
    print(uri)
    jsonString = x.__makejson_inbox__(file, fsrarid, uri)
    get_con.send_json(jsonString, x)
    print(jsonString)
    print(fsrarid, uri, f'{idx + st} / {cnt_checks}', uri[:8])
