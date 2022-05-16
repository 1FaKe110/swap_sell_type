import base64
import json
import xml.etree.ElementTree as xml
import random as ra
import pyodbc
import confluent_kafka as ck

PATH = '/home/ldapusers/gabko/python_scr/'
FILENAME = 'file'


class ParserChequeV3:

    @classmethod
    def __get_chq_attrib__(cls, filename, root, transport_id):
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

        with open(filename) as f:
            chq = f.read()
            ch_bytes = chq.encode('utf-8')
            ch_b64_bytes = base64.b64encode(ch_bytes)
            ch_b64_string = ch_b64_bytes.decode()

        json1 = {"doctype": doctype, "fsrarid": fsrarid, "transportid": transportid, "return": return1, "date": date,
                 "kassa": kassa, "shift": shift, "number": number, "content": {"position": bottles},
                 "data": ch_b64_string}
        return json1, bottles

    def get_chq_attrib(self, filename, root, transport_id):
        return self.__get_chq_attrib__(filename, root, transport_id)


class ParserChequeV0:

    @classmethod
    def __get_chq_attrib__(cls, filename, root, transport_id, fsrar):

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
        returns = {'status': ''}

        for i in root.findall('Bottle'):
            bottle = i.attrib

            if 'volume' in bottle:
                del bottle['volume']

            if '-' in str(bottle['price']):
                returns['status'] = '1'
                bottle['price'] = bottle['price'][1:]
                bottles.append(bottle)
                continue

            returns['status'] = '0'
            bottle['price'] = f"-{bottle['price']}"

            bottles.append(bottle)

        with open(filename) as f:
            chq = f.read()
            ch_bytes = chq.encode('utf-8')
            ch_b64_bytes = base64.b64encode(ch_bytes)
            ch_b64_string = ch_b64_bytes.decode()

        json1 = {"doctype": doctype, "fsrarid": fsrarid, "transportid": transportid, "return": returns['status'], "date": date,
                 "kassa": kassa, "shift": shift, "number": number, "content": {"position": bottles},
                 "data": ch_b64_string}

        print(json1)
        return json1, bottles

    def get_chq_attrib(self, filename, root, transport_id, fsrar):
        return self.__get_chq_attrib__(filename, root, transport_id, fsrar)


class SwapSellType:
    def __init__(self):
        self.__transport_id = None
        self.__filename = __name__

    def set_transport_id(self, value):
        self.__transport_id = value

    def __connect_db__(self):
        server = '31.31.199.107'
        database = 'UnionChequeTWB'
        username = 'zaglushki_read'
        password = 'nEb*hd17QK9dj*2'

        # server = '10.10.4.7'
        # database = 'WBfileStoreCheque'
        # username = 'fileviewer'
        # password = 'qwe123'

        # server = '10.10.4.173'
        # database = 'UnionChequeTWB'
        # username = 'fileviewer'
        # password = 'qwe123'

        driver = 'DRIVER={ODBC Driver 17 for SQL Server};'
        ms = f'PORT=1433;SERVER={server}; DATABASE={database}; UID={username}; PWD={password}'

        with pyodbc.connect(driver + ms) as con:
            with con.cursor() as cursor:
                query = f"SELECT fileData FROM {database}.dbo.Files WITH (NOLOCK) WHERE TransportId = '{self.__transport_id[13:]}'"
                cur_reply = cursor.execute(query)
                columns = [column[0] for column in cur_reply.description]
                results = []
                for row in cur_reply.fetchall():
                    results.append(dict(zip(columns, row)))

        return results

    @classmethod
    def __keyhold__(cls):
        t = 10000
        aint = ra.randint(t, t * t)
        return aint.to_bytes(4, byteorder='big')

    def send_json(self, jsonString):
        srv = 'gitlab-ci.ru:9092'
        # srv = '10.10.4.28:9092'
        # srv = 'prod-kafka-01.nd.fsrar.ru:9092'

        proconf = {'bootstrap.servers': srv, 'client.id': 'gabko'}
        topic = 'inbox'
        producer = ck.Producer(proconf)
        producer.produce(topic, key=self.__keyhold__(), value=jsonString)
        producer.flush()

    def __makejson_inbox__(self, filename):

        def check_docktype(root):
            if 'ChequeV3' in str(root):
                pass
            else:
                chutm = ParserChequeV0()
                type = 'Cheque'
                attr = chutm.get_chq_attrib(filename, root, transport_id=self.__transport_id[13:], fsrar=self.__transport_id[:12])
                return type, attr

        tree = xml.parse(filename)
        root = tree.getroot()

        tpe, attr = check_docktype(root)
        certificate = ''
        sign = ''
        data = attr[0]['data']
        date = attr[0]['date'][:10]
        fsrarid = self.__transport_id[:12]
        json1 = {
            'certificate': certificate,
            'sign': sign,
            'type': tpe,
            'data': data,
            'date': date,
            'fsrarid': fsrarid,
            'uri': self.__transport_id
        }
        jsonString = json.dumps(json1)
        return jsonString


def main():
    while True:

        print("-" * 50,
              "- transport_id format is:\n"
              "\t 020000263354-e1e5714e-12ce-4b03-93bc-9e8fbf0bd5c7\n"
              "")
        transport_id = input("transport_id > ")

        if len(transport_id) != 49:
            print(f"Incorrect length of transport_id. must be 49, yours - {len(transport_id)}\n", '-' * 50)
            continue

        swapper = SwapSellType()
        swapper.set_transport_id(transport_id)

        print("select")
        file = swapper.__connect_db__()[0]['fileData']

        filepath = PATH + FILENAME
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(file.decode('utf-8'))

        print('make json')
        json_string = swapper.__makejson_inbox__(filepath)
        print(json_string)
        print('send to kafka')
        swapper.send_json(json_string)


if __name__ == '__main__':
    main()
