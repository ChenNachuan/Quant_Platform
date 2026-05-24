class ILogSpi(object):
    def __init__(self) -> None:
        pass
    def OnLog(self, level, log, len):
        pass
    def OnLogon(self, data):
        pass

    def OnIndicator(self, indicator):
        pass

    def OnEvent(self, level, code, event_msg):
        pass


class IPushSpi(object):

    def __init__(self, return_df_format = True) -> None:
        self._return_df_format = return_df_format

    def SetDfFormat(self, return_df_format = True):
        self._return_df_format = return_df_format

    def _IsDfFormat(self):
        if self._return_df_format:
            return True
        else:
            return False

    def OnMDSnapshot(self, data, err):
        pass

    def OnMDTickOrder(self, data, err):
        pass

    def OnMDTickExecution(self, data, err):
        pass

    def OnMDOrderQueue(self, data, err):
        pass

    def OnMDIndexSnapshot(self, data, err):
        pass

    def OnMDOptionSnapshot(self, data, err):
        pass

    def OnMDHKTSnapshot(self, data, err):
        pass

    def OnMDAfterHourFixedPriceSnapshot(self, data, err):
        pass

    def OnMDCSIIndexSnapshot(self, data, err):
        pass
    
    def OnMDCnIndexSnapshot(self, data, err):
        pass

    def OnMDHKTRealtimeLimit(self, data, err):
        pass

    def OnMDHKTProductStatus(self, data, err):
        pass
    
    def OnMDHKTVCM(self, data, err):
        pass

    def OnMDFutureSnapshot(self, data, err):
        pass

    def OnKLine(self, data, kline_type, err):
        pass

    def OnSnapshotDerive(self, data, err):
        pass

    def OnFactor(self, data, err):
        pass

    def OnMDOrderBook(self, data, err):
        pass

    def OnMDOrderBookSnapshot(self, data, err):
        pass

    
