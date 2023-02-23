"""
written by DennisXie on 2023-2-18
"""
import re
import sys
import time
import queue
import thosttraderapi as api

def _print(*args, **kwargs):
    print(*args, **kwargs)


class UserConfig(object):
    brokerId: str = ""
    userId: str = ""
    password: str = ""
    appId: str = ""
    authCode: str = ""

    def __init__(self, brokerId: str, userId: str, password: str, appId: str, authCode: str):
        self.brokerId = brokerId
        self.userId = userId
        self.password = password
        self.appId = appId
        self.authCode = authCode


class CTdClient(api.CThostFtdcTraderSpi):
    def __init__(self, userConfig: UserConfig, front: str):
        super().__init__()
        self.tdapi: api.CThostFtdcTraderApi = api.CThostFtdcTraderApi.CreateFtdcTraderApi(userConfig.userId)
        self.userConfig = userConfig
        self.front: str = front
        self.__reqId: int = 0
        self.__ready: bool = False
        self.__today: str = ""
        self.__queue: queue.Queue = queue.Queue()

    @property
    def reqId(self) -> int:
        self.__reqId += 1
        return self.__reqId

    @property
    def ready(self) -> bool:
        return self.__ready

    def connect(self):
        self.tdapi.RegisterSpi(self)
        self.tdapi.SubscribePrivateTopic(api.THOST_TERT_QUICK)
        self.tdapi.SubscribePublicTopic(api.THOST_TERT_QUICK)
        self.tdapi.RegisterFront(self.front)
        self.tdapi.Init()
        while not self.__ready:
            time.sleep(0.2)

    def OnFrontConnected(self):
        """前置连接成功"""
        _print("OnFrontConnected")
        self.authenticate()

    def OnFrontDisconnected(self, nReason):
        "前置断开连接"
        _print(f"Front disconnect, error_code={nReason}")

    def authenticate(self):
        req = api.CThostFtdcReqAuthenticateField()
        req.BrokerID = self.userConfig.brokerId
        req.UserID = self.userConfig.userId
        req.AppID = self.userConfig.appId
        req.AuthCode = self.userConfig.authCode
        self.tdapi.ReqAuthenticate(req, 0)

    def OnRspAuthenticate(self, pRspAuthenticateField: api.CThostFtdcRspAuthenticateField,
                          pRspInfo: api.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        """客户端认证响应"""
        if pRspInfo is not None:
            _print(f"authenticate failed, ErrorID: {pRspInfo.ErrorID}, ErrorMsg: {pRspInfo.ErrorMsg}")

        if pRspInfo is None or pRspInfo.ErrorID == 0:
            self.login()
        else:
            exit(1)

    def login(self):
        req = api.CThostFtdcReqUserLoginField()
        req.BrokerID = self.userConfig.brokerId
        req.UserID = self.userConfig.userId
        req.Password = self.userConfig.password
        req.UserProductInfo = "openctp"
        self.tdapi.ReqUserLogin(req, 0)

    def OnRspUserLogin(self, pRspUserLogin: api.CThostFtdcRspUserLoginField, pRspInfo: api.CThostFtdcRspInfoField,
                       nRequestID: int, bIsLast: bool):
        """登录响应"""
        if pRspInfo is not None:
            _print(f"login failed, ErrorID: {pRspInfo.ErrorID}, ErrorMsg: {pRspInfo.ErrorMsg}")

        if pRspInfo is None or pRspInfo.ErrorID == 0:
            self.__ready = True
            self.__today = pRspUserLogin.TradingDay
        else:
            exit(1)

    def querySettlementInfo(self, tradingDay: str) -> str:
        _print(f"query settlement {self.userConfig}")
        req = api.CThostFtdcQrySettlementInfoField()
        req.BrokerID = self.userConfig.brokerId
        req.TradingDay = tradingDay
        req.InvestorID = self.userConfig.userId
        self.tdapi.ReqQrySettlementInfo(req, self.reqId)

        content: str = ""
        chunks: list[api.CThostFtdcSettlementInfoField] = []
        last = False
        while not last:
            chunk, last = self.__queue.get()
            content = content + chunk
        return content

    def OnRspQrySettlementInfo(self, pSettlementInfo: api.CThostFtdcSettlementInfoField,
                               pRspInfo: api.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        if pRspInfo is not None:
            _print(f"query settlement failed, ErrorID: {pRspInfo.ErrorID}, ErrorMsg: {pRspInfo.ErrorMsg}")

        if pSettlementInfo is not None:
            self.__queue.put_nowait((str(pSettlementInfo.Content), bIsLast))
        else:
            _print(f"empty settlement content, last={bIsLast}")


class SectionHandler(object):

    TITLE = ""

    def parse(self, contents: list[str]):
        """不能处理的行直接略过"""
        pass


class SettlementStatementHandler(SectionHandler):

    TITLE = "交易结算单"

    CLIENT_ID_KEY = "ClientID"
    DATE_KEY = "Date"
    DETAILS_KEY = "SettlementStatement"

    def __init__(self) -> None:
        super().__init__()
        self.client_id_pattern: re.Pattern = re.compile(r"Client ID：\s*(?P<ClientID>\d+)")
        self.date_pattern: re.Pattern = re.compile(r"Date：\s*(?P<Date>\d+)")
        # 匹配 1-n个(英文字母、空白符、/)：0-n个(空白符)1-n个(数字).1-n个(数字)0-1个(%)
        self.detail_pattern: re.Pattern = re.compile(r"[a-zA-Z\s/]+：\s*\d+\.\d+")
        self.result = {
            self.CLIENT_ID_KEY: None,
            self.DATE_KEY: None,
            self.DETAILS_KEY: {}
        }
    
    def parse(self, contents: list[str]):
        for line in contents:
            if not self.result[self.CLIENT_ID_KEY]:
                self.parse_client_id(line)
            elif not self.result[self.DATE_KEY]:
                self.parse_date(line)
            else:
                self.parse_details(line)
        return self.result

    def parse_client_id(self, line: str):
        match = self.client_id_pattern.search(line)
        if match:
            self.result[self.CLIENT_ID_KEY] = match.group(self.CLIENT_ID_KEY)
    
    def parse_date(self, line: str):
        match = self.date_pattern.search(line)
        if match:
            self.result[self.DATE_KEY] = match.group(self.DATE_KEY)

    def parse_details(self, line: str):
        matches: list[str] = self.detail_pattern.findall(line)
        if matches:
            for match in matches:
                compactMatch = match.replace(" ", "")    # Aaa Bbb:  95.2 -> AaaBbb:95.2
                kv = compactMatch.split("：")            # AaaBbb:95.2 -> ['AaaBbb', '95.2']
                self.result[self.DETAILS_KEY][kv[0]] = float(kv[1])

class TableStatus(object):
    NONE = "None"
    HEADER = "header"
    DETAILS = "details"
    TOTAL = "total"
    COMMENT = "comment"


class TableHandler(SectionHandler):

    TITLE = ""
    KEY = "Table"

    split_line = "------------------"
    status = [TableStatus.NONE, TableStatus.HEADER, TableStatus.DETAILS, TableStatus.TOTAL, TableStatus.COMMENT]
    handlers = {}
    result = {}

    HEADER_KEY = "Headers"
    DETAILS_KEY = "Details"
    TOTAL_KEY = "Total"
    COMMENT_KEY = "Comment"

    def __init__(self) -> None:
        super().__init__()
    
    def parse(self, contents: list[str]) -> dict[str, any]:
        currentStatusIdx = 0
        currentStatus = self.status[currentStatusIdx]
        for i in range(len(contents)):
            if contents[i].startswith(self.split_line):
                currentStatusIdx += 1
                currentStatus = self.status[currentStatusIdx]
                continue
            elif currentStatus in self.handlers:
                self.handlers[currentStatus](contents[i])
        return self.result

    def parse_header(self, line: str):
        pass

    def parse_detail(self, line: str) -> dict[str, any]:
        pass

    def parse_total(self, line: str) -> dict[str, any]:
        pass

    def parse_comment(self, line: str) -> dict[str, str]:
        pass


class TransactionsHandler(TableHandler):

    TITLE = "成交记录"
    KEY = "Transactions"

    def __init__(self) -> None:
        super().__init__()
        self.handlers = {
            TableStatus.DETAILS: self.parse_detail,
        }
        self.result = {
            self.KEY: []
        }
    
    def parse_detail(self, line: str) -> dict[str, any]:
        compactLine = line.replace(" ", "")[1:-1]
        cells = compactLine.split("|")
        self.result[self.KEY].append({
            "Date": cells[0],
            "InvestUnit": cells[1],
            "Exchange": cells[2],
            "TradingCode": cells[3],
            "Product": cells[4],
            "Instrument": cells[5],
            "B/S": cells[6],
            "S/H": cells[7],
            "Price": float(cells[8]),
            "Lots": int(cells[9]),
            "Turnover": float(cells[10]),
            "O/C": cells[11],
            "Fee": float(cells[12]),
            "RealizedP/L": float(cells[13]),
            "PremiumReceived/Paid": float(cells[14]),
            "TransactionNo": cells[15],
            "AccountID": cells[16]
        })


class PositionsClosedHandler(TableHandler):

    TITLE = "平仓明细"
    KEY = "PositionsClosed"

    def __init__(self) -> None:
        super().__init__()
        self.handlers = {
            TableStatus.DETAILS: self.parse_detail,
        }
        self.result = {
            self.KEY: []
        }
    
    def parse_detail(self, line: str) -> dict[str, any]:
        compactLine = line.replace(" ", "")[1:-1]
        cells = compactLine.split("|")
        self.result[self.KEY].append({
            "Date": cells[0],
            "InvestUnit": cells[1],
            "Exchange": cells[2],
            "TradingCode": cells[3],
            "Product": cells[4],
            "Instrument": cells[5],
            "OpenDate": cells[6],
            "S/H": cells[7],
            "B/S": cells[8],
            "Lots": int(cells[9]),
            "PosOpenPrice": float(cells[10]),
            "PrevSettle": float(cells[11]),
            "TransPrice": float(cells[12]),
            "RealizedP/L": float(cells[13]),
            "PremiumReceived/Paid": float(cells[14]),
            "AccountID": cells[15]
        })

class PositionsDetailHandler(TableHandler):

    TITLE = "持仓明细"
    KEY = "PositionsDetail"

    def __init__(self) -> None:
        super().__init__()
        self.handlers = {
            TableStatus.DETAILS: self.parse_detail,
        }
        self.result = {
            self.KEY: []
        }
    
    def parse_detail(self, line: str) -> dict[str, any]:
        compactLine = line.replace(" ", "")[1:-1]
        cells = compactLine.split("|")
        self.result[self.KEY].append({
            "InvestUnit": cells[0],
            "Exchange": cells[1],
            "TradingCode": cells[2],
            "Product": cells[3],
            "Instrument": cells[4],
            "OpenDate": cells[5],
            "S/H": cells[6],
            "B/S": cells[7],
            "Position": int(cells[8]),
            "PosOpenPrice": float(cells[9]),
            "PrevSettle": float(cells[10]),
            "SettlementPrice": float(cells[11]),
            "AccumP/L": float(cells[12]),
            "MTMP/L": float(cells[13]),
            "Margin": float(cells[14]),
            "MarketValueOptions": float(cells[15]),
            "AccountID": cells[16]
        })

class PositionsHandler(TableHandler):

    TITLE = "持仓汇总"
    KEY = "Positions"

    def __init__(self) -> None:
        super().__init__()
        self.handlers = {
            TableStatus.DETAILS: self.parse_detail,
        }
        self.result = {
            self.KEY: []
        }
    
    def parse_detail(self, line: str) -> dict[str, any]:
        compactLine = line.replace(" ", "")[1:-1]
        cells = compactLine.split("|")
        self.result[self.KEY].append({
            "InvestUnit": cells[0],
            "TradingCode": cells[1],
            "Product": cells[2],
            "Instrument": cells[3],
            "LongPos": int(cells[4]),
            "AvgBuyPrice": float(cells[5]),
            "ShortPos": int(cells[6]),
            "AvgCellPrice": float(cells[7]),
            "PrevSettle": float(cells[8]),
            "SettleToday": float(cells[9]),
            "MTMP/L": float(cells[10]),
            "MarginOccupied": float(cells[11]),
            "S/H": cells[12],
            "MarketValue(Long)": float(cells[13]),
            "MarketValue(Short)": float(cells[14]),
            "AccountID": cells[15],
        })


class SettlementParser(object):

    HEADER = 0
    MTM = 1
    POSITIONS_DETAILS = 2
    POSITIONS = 3

    def __init__(self, line_spliter="\r\n"):
        self._line_spliter = line_spliter
        self._handlers: dict[str, SectionHandler] = {
            SettlementStatementHandler.TITLE: SettlementStatementHandler(),
            TransactionsHandler.TITLE: TransactionsHandler(),
            PositionsClosedHandler.TITLE: PositionsClosedHandler(),
            PositionsDetailHandler.TITLE: PositionsDetailHandler(),
            PositionsHandler.TITLE: PositionsHandler()
        }

    def parse(self, content: str):
        contentLines = content.split(self._line_spliter)
        sections = self._split_to_section(contentLines)
        parsed = dict()
        for title, section in sections.items():
            start, end = section
            if title in self._handlers:
                result = self._handlers[title].parse(contentLines[start:end])
                parsed.update(result)
        return parsed

    def _split_to_section(self, contentLines: list[str]) -> dict[str, (int, int)]:
        section_start = 0
        current_section = None
        sections: dict[str, (int, int)] = {}
        checked = {
            current_section: 1
        }
        for i in range(len(contentLines)):
            for title in self._handlers.keys():
                if title not in checked and contentLines[i].find(title) >= 0:
                    sections[current_section] = (section_start, i)
                    checked[title] = 1
                    current_section = title
                    section_start = i
                    break
        else:
            if section_start < len(contentLines) and current_section not in sections:
                sections[current_section] = (section_start, len(contentLines))
        return sections


if __name__ == "__main__":
    front = "tcp://180.168.146.187:10201"
    # brokerId, userId, password, appId, authCode
    user = UserConfig(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    client = CTdClient(user, front)
    client.connect()
    print(client.querySettlementInfo("20230215"))
