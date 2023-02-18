"""
written by DennisXie on 2023-2-18
"""
import sys
import time
import thosttraderapi as api


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
        self.tdapi: api.CThostFtdcTraderApi = api.CThostFtdcTraderApi.CreateFtdcTraderApi(f"connect\\{userConfig.userId}")
        self.userConfig = userConfig
        self.front: str = front
        self.__reqId: int = 0
        self.__ready: bool = False
        self.__today: str = ""

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
        print("OnFrontConnected")
        self.authenticate()

    def OnFrontDisconnected(self, nReason):
        "前置断开连接"
        print(f"Front disconnect, error_code={nReason}")

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
            print(f"authenticate failed, ErrorID: {pRspInfo.ErrorID}, ErrorMsg: {pRspInfo.ErrorMsg}")

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
            print(f"login failed, ErrorID: {pRspInfo.ErrorID}, ErrorMsg: {pRspInfo.ErrorMsg}")

        if pRspInfo is None or pRspInfo.ErrorID == 0:
            self.__ready = True
            self.__today = pRspUserLogin.TradingDay
        else:
            exit(1)

    def querySettlementInfo(self, tradingDay: str):
        print(f"query settlement {self.userConfig}")
        req = api.CThostFtdcQrySettlementInfoField()
        req.BrokerID = self.userConfig.brokerId
        req.TradingDay = tradingDay
        req.InvestorID = self.userConfig.userId
        self.tdapi.ReqQrySettlementInfo(req, self.reqId)

    def OnRspQrySettlementInfo(self, pSettlementInfo: api.CThostFtdcSettlementInfoField,
                               pRspInfo: api.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        if pRspInfo is not None:
            print(f"query settlement failed, ErrorID: {pRspInfo.ErrorID}, ErrorMsg: {pRspInfo.ErrorMsg}")

        if pSettlementInfo is not None:
            print(pSettlementInfo.Content)
        else:
            print("empty settlement content")

    def settlementConfirm(self):
        # TODO: 是否要干掉confirm
        req = api.CThostFtdcSettlementInfoConfirmField()
        req.BrokerID = self.userConfig.brokerId
        req.InvestorID = self.userConfig.userId
        self.tdapi.ReqSettlementInfoConfirm(req, self.reqId)

    def OnRspSettlementInfoConfirm(self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast):
        if pRspInfo is not None:
            print(f"confirm failed, ErrorID: {pRspInfo.ErrorID}, ErrorMsg: {pRspInfo.ErrorMsg}")
        else:
            print("confirm success")
            self.__ready = True

    def OnRspQryInstrument(self, pInstrument: api.CThostFtdcInstrumentField,
                           pRspInfo: api.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        """ 查询合约应答 """
        if pRspInfo is not None:
            print(f"OnRspQryInstrument: ErrorID={pRspInfo.ErrorID}, ErrorMsg={pRspInfo.ErrorMsg}")

        print(f"OnRspQryInstrument: InstrumentID={pInstrument.InstrumentID}, "
              f"ExchangeID={pInstrument.ExchangeID}, PriceTick={pInstrument.PriceTick}, "
              f"ProductID={pInstrument.ProductID}, ExpireDate={pInstrument.ExpireDate}")


if __name__ == "__main__":
    front = "tcp://180.168.146.187:10130"
    # brokerId, userId, password, appId, authCode
    user = UserConfig(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    client = CTdClient(user, front)
    client.connect()
    client.querySettlementInfo("20230217")
    i = 0
    while i < 10:
        time.sleep(1)
        i += 1
