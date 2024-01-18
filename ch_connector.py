import pandas as pd
import requests
import gzip
from io import BytesIO


class CHConnector:
    def __init__(self, url: str, port=8123, auth=('user', 'pass')):
        self._url = url + ":" + str(port)

        self._user = auth[0]
        self._pass = auth[1]

        self._session = requests.Session()
        self._session.auth = (self._user, self._pass)

    def __is_select_query(self, query):
        if "select" in query.lower() or "on cluster" in query.lower():
            return True
        else:
            return False

    def __to_pandas(self, content, separator="\t", header_line=0):
        DATA = BytesIO(content)
        df = pd.read_csv(DATA, sep=separator, header=header_line)
        return df

    def __check_status(self, response):
        if response.status_code != 200:
            raise Exception(f"Status code is {response.status_code}: {response.text}")
        else:
            return True

    def ping(self):
        req = self._session.get(self._url, params={"query": "SELECT 1"})
        self.__check_status(req)
        print(f"Pinging {self._url}: {req.elapsed}")

    def show_dbs(self):
        req = self._session.get(self._url, params={"query": "SHOW DATABASES"})
        self.__check_status(req)
        return req.text

    def show_tables(self, database):
        req = self._session.post(
            self._url, params={"query": "SHOW TABLES FROM " + database}
        )
        self.__check_status(req)
        return req.text

    def query(self, query, is_raw=False, format="TabSeparatedWithNames"):
        req = self._session.post(
            self._url, params={"query": query + " format " + format}
        )
        self.__check_status(req)

        if self.__is_select_query(query) and not is_raw:
            return self.__to_pandas(req.content, header_line=0)
        else:
            return req.content

    def select(self, query, format="TabSeparatedWithNames"):
        req = self._session.post(
            self._url, params={"query": query + " format " + format}
        )
        self.__check_status(req)

        return self.__to_pandas(req.content, header_line=0)

    def query_compressed(self, query, is_raw=False, format="TabSeparatedWithNames"):
        req = self._session.post(
            self._url,
            params={
                "query": query + " format " + format,
                "enable_http_compression": "1",
            },
            headers={"Accept-Encoding": "gzip"},
        )
        self.__check_status(req)

        if self.__is_select_query(query) and not is_raw:
            return self.__to_pandas(req.content)
        else:
            return req.content

    def insert(
        self, table: str, dataframe: pd.DataFrame = pd.DataFrame(), format="CSV"
    ):
        DATA = BytesIO()
        dataframe.to_csv(DATA, encoding="UTF-8", header=False, index=False)
        DATA.seek(0)

        req = self._session.post(
            self._url,
            params={"query": f"INSERT INTO {table} FORMAT {format}"},
            data=DATA,
        )
        return self.__check_status(req)

    def clear_table(self, table: str):
        req = self._session.post(
            self._url, params={"query": f"TRUNCATE TABLE {table} ON CLUSTER ba"}
        )
        return self.__check_status(req)

    def insert_compressed(self, table: str, dataframe: pd.DataFrame, format="CSV"):
        DATA = BytesIO()
        match format:
            case "CSV":
                dataframe.to_csv(DATA, encoding="UTF-8", header=False, index=False)
            # case 'JSON':
            # dataframe.to_json(DATA, index=False)

        DATA.seek(0)
        DATA_COMPRESSED = gzip.compress(DATA.read())
        req = self._session.post(
            self._url,
            params={
                "query": f"INSERT INTO {table} FORMAT {format}",
                "enable_http_compression": "1",
            },
            data=DATA_COMPRESSED,
            headers={"Content-Encoding": "gzip"},
        )
        return self.__check_status(req)
