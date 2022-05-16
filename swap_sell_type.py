import base64
import json
import xml.etree.ElementTree as xml
import random as ra

import pyodbc
import confluent_kafka as ck

PATH = '/home/ldapusers/gabko/python_scr/'  # path for saving
FILENAME = 'broken_file.xml'  # filename for chq if scr can't parse it


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


class SwapSellType:
    def __init__(self):
        self.__transport_id = None

    def set_transport_id(self, value):
        self.__transport_id = value

    def __connect_db__(self):
        server = '31.31.199.107'  # песок
        database = 'UnionChequeTWB'  # песок
        username = 'zaglushki_read'  # песок
        password = 'nEb*hd17QK9dj*2'  # песок

        # server = '10.10.4.7'              # тест
        # database = 'WBfileStoreCheque'    # тест
        # username = 'fileviewer'           # тест
        # password = 'qwe123'               # тест

        # server = '10.10.4.173'            # прод
        # database = 'UnionChequeTWB'       # прод
        # username = 'fileviewer'           # прод
        # password = 'qwe123'               # прод

        driver = 'DRIVER={ODBC Driver 17 for SQL Server};'
        ms = f'PORT=1433;SERVER={server}; DATABASE={database}; UID={username}; PWD={password}'

        with pyodbc.connect(driver + ms) as con:
            with con.cursor() as cursor:
                query = f"SELECT fileData FROM {database}.dbo.Files WITH (NOLOCK) WHERE TransportId = '{self.__transport_id[13:]}'"  # select from db by transport_id
                cur_reply = cursor.execute(query)
                columns = [column[0] for column in cur_reply.description]  # get column name from db_reply
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
        srv = 'gitlab-ci.ru:9092'  # песок
        # srv = '10.10.4.28:9092'                   # тест
        # srv = 'prod-kafka-01.nd.fsrar.ru:9092'    # прод

        proconf = {'bootstrap.servers': srv, 'client.id': 'gabko'}
        topic = 'inbox'
        producer = ck.Producer(proconf)
        producer.produce(topic, key=self.__keyhold__(), value=jsonString)
        producer.flush()

    def __makejson_inbox__(self, filename):

        def check_doctype():
            if 'ChequeV3' in str(root):  # cheque v3 is not supported
                pass
            else:
                chutm = ParserChequeV0()
                type = 'Cheque'
                attr = chutm.get_chq_attrib(filename, root)  # get date and date from cheque
                return type, attr

        try:  # try parse from file
            print("parse from file")
            tree = xml.parse(filename)
            root = tree.getroot()

        except xml.ParseError as e:  # if scr can't parse from file: read file -> save to string -> parse from string
            print(e)
            print("read file and parse from string")
            with open(filename) as f:
                file = f.read()
            root = xml.fromstring(file)

        tpe, attr = check_doctype()
        certificate = ''
        sign = ''
        data = attr['data']
        date = attr['date'][:10]
        fsrarid = self.__transport_id[:12]
        transport = self.__transport_id[:-1] + '1' if self.__transport_id[-1] == '0' else self.__transport_id[:-1] + "0"  # if last char of tr_id is 0 -> 1, else: x -> 0
        json1 = {  # dict for inbox
            'certificate': certificate,
            'sign': sign,
            'type': tpe,
            'data': data,
            'date': date,
            'fsrarid': fsrarid,
            'uri': transport
        }
        jsonString = json.dumps(json1)  # make json from dict
        return jsonString


def operate(transport_id):

    if len(transport_id) != 49:
        print(f"Incorrect length of transport_id. must be 49, yours - {len(transport_id)}\n", '-' * 50)  # if transport id length is < 49 -> next element from list
        return

    swapper = SwapSellType()
    swapper.set_transport_id(transport_id)

    print("select")
    try:  # try to fetch from db, if result is empty -> next()
        file = swapper.__connect_db__()[0]['fileData']
    except IndexError:
        print("empty select, try another file")
        return

    filepath = PATH + FILENAME  # file path to save file near script
    with open(filepath, 'w') as f:
        f.write(file.decode('utf-8'))

    print('make json')
    json_string = swapper.__makejson_inbox__(filepath)  # make json for inbox
    print(json_string)
    print('send to kafka')
    # swapper.send_json(json_string)  # send json to inbox
    print('-' * 50, '\n')


def main():
    choose = int(input("0 - read from file\n"
                       "1 - input by uri\n"
                       " > "))
    if choose == 0:
        file_name = input('{filename}.{format} \n'
                          ' > ')
        with open(file_name) as userdata:  # read users with uris json
            uris = json.load(userdata)

        uris_length = len(uris)
        for idx, uri in enumerate(uris):

            print(uri[13:], idx + 1, uris_length, sep=" / ")
            operate(uri)

        main()
    else:
        while True:
            print("-" * 50,
                  "\n- transport_id format is:\n"
                  "\t 020000263354-e1e5714e-12ce-4b03-93bc-9e8fbf0bd5c7\n"
                  "q -  for exit\n")
            transport_id = input("transport_id > ")

            if transport_id == 'q':
                main()

            operate(transport_id)


if __name__ == '__main__':
    main()
