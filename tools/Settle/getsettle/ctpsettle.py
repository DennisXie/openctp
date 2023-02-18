"""
written by DennisXie on 2023-2-18
"""

import time
import thosttraderapi as api


class UserConfig(object):

    borkerId: str = ""
    userId: str = ""
    password: str = ""
    appId: str = ""
    authCode: str = ""


class CTdClient(api.CThostFtdcTraderSpi):
    def __init__(self, tdapi: api.CThostFtdcTraderApi, userConfig: UserConfig, front: str):
        super().__init__()
        self.tdapi: api.CThostFtdcTraderApi = tdapi
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

    def init(self):
        self.tdapi.RegisterSpi(self)
        self.tdapi.SubscribePrivateTopic(api.THOST_TERT_QUICK)
        self.tdapi.SubscribePublicTopic(api.THOST_TERT_QUICK)
        self.tdapi.RegisterFront(self.front)
        self.tdapi.Init()
        while not self.__ready:
            time.sleep(0.2)

    def querySettlementInfo(self, tradingDay: str, currencyId: str = "CNY"):
        req = api.CThostFtdcQrySettlementInfoField()
        req.BrokerID = self.userConfig.brokerId
        # req.TradingDay = tradingDay
        req.InvestorID = self.userConfig.userId
        # req.AccountID = self.userConfig.userId
        # req.CurrencyID = currencyId
        self.tdapi.ReqQrySettlementInfo(req, self.reqId)

    def OnFrontConnected(self):
        """前置连接成功"""
        print("OnFrontConnected")
        self.authenticate()

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
        print(f"authenticate {pRspInfo}")
        if pRspInfo is not None:
            print(f"authenticate failed, ErrorID: {pRspInfo.ErrorID}, ErrorMsg: {pRspInfo.ErrorMsg}")
        # else:
            self.login()

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
        # else:
            print("login success")
            # self.__ready = True
            self.__today = pRspUserLogin.TradingDay
            self.settlementConfirm()

    def settlementConfirm(self):
        req = api.CThostFtdcSettlementInfoConfirmField()
        req.BrokerID = self.userConfig.borkerId
        req.InvestorID = self.userConfig.userId
        self.tdapi.ReqSettlementInfoConfirm(req, 0)
        # self.tdapi.ReqSettlementInfoConfirm(req, self.reqId)

    def OnRspSettlementInfoConfirm(self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast):
        if pRspInfo is not None and pRspInfo.ErrorID != 0:
            print(f"confirm failed, ErrorID: {pRspInfo.ErrorID}, ErrorMsg: {pRspInfo.ErrorMsg}")
        else:
            print("confirm success")
            self.__ready = True

    def OnRspQrySettlementInfo(self, pSettlementInfo: api.CThostFtdcSettlementInfoField,
                               pRspInfo: api.CThostFtdcRspInfoField, nRequestID: int, bIsLast: bool):
        if pRspInfo is not None:
            print(f"query settlement failed, ErrorID: {pRspInfo.ErrorID}, ErrorMsg: {pRspInfo.ErrorMsg}")

        if pSettlementInfo is not None:
            print(pSettlementInfo.Content)
        else:
            print(pSettlementInfo)
            print("pSettlementInfo null")

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
    user = UserConfig()
    user.brokerId = "9999"
    user.authCode = "0000000000000000"
    user.appId = "simnow_client_test"
    user.userId = "203199"
    user.password = ""

    tdapi = api.CThostFtdcTraderApi.CreateFtdcTraderApi(user.userId)
    client = CTdClient(tdapi, user, front)
    client.init()
    client.querySettlementInfo("20230215")
    i = 0
    while i < 10:
        time.sleep(1)
        i += 1
