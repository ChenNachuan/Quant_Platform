import tgw
import pandas as pd
import json
try:
    pd.json_normalize
    from pandas import json_normalize
except Exception as e:
    from pandas.io.json import json_normalize
from tgw.base_struct import *
"""
文件描述：整个文件的spi定义与实现都是属于中间层，相当于适配器，打通真正python接口与c++转出来的python接口
注意点：
    对于查询：
        1.要继承c++转出来的基类，实现一个TmpSpi
        2.定义一个WaitSpi类
        3.在1中实现的类调用对应的WaitSpi类，实现同步逻辑。核心是用到threading.Event()
"""

class TmpPushSpi(tgw.IGMDSpi):
    """
    功能描述：推送spi中间层spi，充当适配器
    """
    def __init__(self):
        super().__init__()
        self._spi = None
        self._log_spi = None

    def SetSpi(self, spi):
        self._spi = spi

    def SetLogSpi(self, spi):
        self._log_spi = spi

    def OnLog(self, level, log, len):
        if self._log_spi:
            self._log_spi.OnLog(level, log, len)
    def OnLogon(self, data):
        if self._log_spi:
            self._log_spi.OnLogon(data)
        tgw.IGMDApi_FreeMemory(data)

    def OnIndicator(self, indicator, len):
        if self._log_spi:
            self._log_spi.OnIndicator(indicator)

    def OnEvent(self, level, code, event_msg, len):
        if self._log_spi:
            self._log_spi.OnEvent(level, code, event_msg)


    def OnMDSnapshot(self, data, cnt):
        try:
            if not self._spi is None:
                for i in range(cnt):
                    if isinstance(data, tgw.MDSnapshotL2):
                        snapshot = TGWSnapshotL2(data,i)
                        self._spi.OnMDSnapshot(snapshot, '')
                    else:
                        snapshot = TGWSnapshotL1(data,i)
                        self._spi.OnMDSnapshot(snapshot, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDSnapshot(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(data)

    def OnMDOrderQueue(self, data, cnt):
        try:
            if not self._spi is None:
                for i in range(cnt):
                    order_queue = TGWOrderQueue(data,i)
                    self._spi.OnMDOrderQueue(order_queue, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDOrderQueue(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(data)

    def OnMDTickOrder(self, data, cnt):
        try:
            if not self._spi is None:
                for i in range(cnt):
                    tick_order = TGWTickOrder(data,i)
                    self._spi.OnMDTickOrder(tick_order, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDTickOrder(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(data)

    def OnMDTickExecution(self, data, cnt):
        try:
            if not self._spi is None:
                for i in range(cnt):
                    tick_exe = TGWTickExecution(data,i)
                    self._spi.OnMDTickExecution(tick_exe, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDTickExecution(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(data)

    def OnMDIndexSnapshot(self, data, cnt):
        try:
            if not self._spi is None:
                for i in range(cnt):
                    snapshot = TGWIndexSnapshot(data,i)
                    self._spi.OnMDIndexSnapshot(snapshot, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDIndexSnapshot(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(data)

    def OnKLine(self, kline, cnt, kline_type):
        try:
            if not self._spi is None:
                for i in range(cnt):
                    kline_struct = TGWKLine(kline,i)
                    self._spi.OnKLine(kline_struct, kline_type, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnKLine(None, kline_type ,e)
        finally:
            tgw.IGMDApi_FreeMemory(kline)
 
    def OnMDOptionSnapshot(self, data, cnt):
        try:
            if not self._spi is None:
                for i in range(cnt):
                    snapshot = TGWOptionSnapshot(data,i)
                    self._spi.OnMDOptionSnapshot(snapshot, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDOptionSnapshot(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(data)

    def OnMDHKTSnapshot(self, data, cnt):
        try:
            if not self._spi is None:
                for i in range(cnt):
                    snapshot = TGWHKTSnapshot(data,i)
                    self._spi.OnMDHKTSnapshot(snapshot, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDHKTSnapshot(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(data)

    def OnMDAfterHourFixedPriceSnapshot(self, data, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_AfterHourFixedPriceSnapshotToJson(data, cnt)
                json_list = json.loads(json_str)
                if self._spi._IsDfFormat():
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi.OnMDAfterHourFixedPriceSnapshot(result, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDAfterHourFixedPriceSnapshot(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(data)

    def OnMDCSIIndexSnapshot(self, data, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_CSIIndexSnapshotToJson(data, cnt)
                json_list = json.loads(json_str)
                if self._spi._IsDfFormat():
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi.OnMDCSIIndexSnapshot(result, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDCSIIndexSnapshot(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(data)

    def OnMDCnIndexSnapshot(self, data, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_CnIndexSnapshotToJson(data, cnt)
                json_list = json.loads(json_str)
                if self._spi._IsDfFormat():
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi.OnMDCnIndexSnapshot(result, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDCnIndexSnapshot(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(data)

    def OnMDHKTRealtimeLimit(self, data, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_HKTRealtimeLimitToJson(data, cnt)
                json_list = json.loads(json_str)
                if self._spi._IsDfFormat():
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi.OnMDHKTRealtimeLimit(result, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDHKTRealtimeLimit(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(data)

    def OnMDHKTProductStatus(self, data, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_HKTProductStatusToJson(data, cnt)
                json_list = json.loads(json_str)
                if self._spi._IsDfFormat():
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi.OnMDHKTProductStatus(result, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDHKTProductStatus(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(data)

    def OnMDHKTVCM(self, data, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_HKTVCMToJson(data, cnt)
                json_list = json.loads(json_str)
                if self._spi._IsDfFormat():
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi.OnMDHKTVCM(result, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDHKTVCM(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(data)

    def OnMDFutureSnapshot(self, data, cnt):
        try:
            if not self._spi is None:
                for i in range(cnt):
                    future_shap = TGWFutureSnapshot(data,i)
                    self._spi.OnMDFutureSnapshot(future_shap, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDFutureSnapshot(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(data)

    def OnSnapshotDerive(self, snapshot_derive, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_SnapshotDeriveToJson(snapshot_derive, cnt)
                json_list = json.loads(json_str)
                if self._spi._IsDfFormat():
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi.OnSnapshotDerive(result, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnSnapshotDerive(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(snapshot_derive)
    
    def OnFactor(self, factor):
        """
        因子不支持dataframe格式
        """
        try:
            if not self._spi is None:
                json_str = tgw.Tools_FactorToJson(factor, 1)
                json_list = json.loads(json_str)
                # if self._spi._IsDfFormat():
                #     result = json_normalize(json_list)
                # else:
                #     result = json_list
                result = json_list
                self._spi.OnFactor(result, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnFactor(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(factor)


    def OnMDOrderBook(self, order_book):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_OrderBookToJson(order_book)
                json_list = json.loads(json_str)
                if self._spi._IsDfFormat():
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi.OnMDOrderBook(result, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDOrderBook(None, str(e))
        finally:
            pass
    
    def OnMDOrderBookSnapshot(self, order_book_snapshots, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_OrderBookSnapshotToJson(order_book_snapshots, cnt)
                json_list = json.loads(json_str)
                if self._spi._IsDfFormat():
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi.OnMDOrderBookSnapshot(result, '')
        except Exception as e:
            if not self._spi is None:
                self._spi.OnMDOrderBookSnapshot(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(order_book_snapshots)       

class TmpQuerySnapshotSpi(tgw.IGMDSnapshotSpi):
    """
    功能描述：查询快照spi，中间层spi，充当适配器。配合TmpQuerySnapshotWaitSpi对象使用
    """
    def __init__(self, wait_event = None, return_df_format = True):
        super().__init__()
        self._spi = None
        self._wait_event = wait_event
        self._return_df_format = return_df_format


    def SetSpi(self, spi):
        self._spi = spi

    def OnMDSnapshotL1(self, snapshots, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_SnapshotL1ToJson(snapshots, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)

        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(snapshots)
            if self._wait_event:
                self._wait_event.set()

    def OnMDSnapshotL2(self, snapshots, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_SnapshotL2ToJson(snapshots, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)

        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(snapshots)
            if self._wait_event:
                self._wait_event.set()

    def OnMDIndexSnapshot(self, index_snapshots, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_IndexSnapshotToJson(index_snapshots, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)

        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(index_snapshots)
            if self._wait_event:
                self._wait_event.set()

    def OnMDOptionSnapshot(self, opt_snapshots, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_OptionSnapshotToJson(opt_snapshots, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)

        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(opt_snapshots)
            if self._wait_event:
                self._wait_event.set()

    def OnMDFutureSnapshot(self, future_ticks, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_FutureSnapshotToJson(future_ticks, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)

        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(future_ticks)
            if self._wait_event:
                self._wait_event.set()

    def OnMDHKTSnapshot(self, hkt_snapshots, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_HKTSnapshotToJson(hkt_snapshots, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)

        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(hkt_snapshots)
            if self._wait_event:
                self._wait_event.set() 

    def OnMDHKExOrderSnapshot(self, order_snapshot, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_OrderSnapshotToJson(order_snapshot, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)

        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(order_snapshot)
            if self._wait_event:
                self._wait_event.set() 

    def OnMDHKExOrderBrokerSnapshot(self, order_broker_snapshot, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_OrderBrokerSnapshotToJson(order_broker_snapshot, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)

        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(order_broker_snapshot)
            if self._wait_event:
                self._wait_event.set() 

    def OnStatus(self, status):
        if not self._spi is None:
            self._spi(None, status.error_code) 
        tgw.IGMDApi_FreeMemory(status)
        if self._wait_event:
            self._wait_event.set()


class TmpQuerySnapshotWaitSpi(object):
    """
    功能描述：查询快照spi，实现同步查询接口的基础，配合TmpQuerySnapshotSpi实现同步查询
    """
    def __init__(self):
        super().__init__()
        self._result = None
        self._err = tgw.ErrorCode.kSuccess
    def GetResult(self):
        return self._result, self._err

    def OnResponse(self, result, status):
        if not result is None:
            self._result = result
        if not status is None:
            if  isinstance(status, str):
                self._err = status
            elif  isinstance(status, int):
                self._err = status


class TmpQueryKlineSpi(tgw.IGMDKlineSpi):
    """
    功能描述：查询k线spi，中间层spi，充当适配器。配合TmpQueryKlineWaitSpi对象使用
    """
    def __init__(self, wait_event = None, return_df_format = True):
        super().__init__()
        self._spi = None
        self._wait_event = wait_event
        self._return_df_format = return_df_format

    def SetSpi(self, spi):
        self._spi = spi

    def OnMDKLine(self, klines, cnt, kline_type):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_KLineToJson(klines, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)

        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(klines)
            if self._wait_event:
                self._wait_event.set()

    def OnStatus(self, status):
        if not self._spi is None:
            self._spi(None, status.error_code) 
        tgw.IGMDApi_FreeMemory(status)
        if self._wait_event:
            self._wait_event.set() 


class TmpQueryKlineWaitSpi(object):
    """
    功能描述：查询k线spi，实现同步查询接口的基础，配合TmpQueryKlineSpi实现同步查询
    """
    def __init__(self):
        super().__init__()
        self._result = None
        self._err = tgw.ErrorCode.kSuccess
    def GetResult(self):
        return self._result, self._err

    def OnResponse(self, result, status):
        if not result is None:
            self._result = result
        if not status is None:
            if  isinstance(status, str):
                self._err = status
            elif  isinstance(status, int):
                self._err = status     


class TmpQueryOrderQueueSpi(tgw.IGMDOrderQueueSpi):
    """
    功能描述：查询委托队列spi，中间层spi，充当适配器。配合TmpQueryOrderQueueWaitSpi对象使用
    """
    def __init__(self, wait_event = None, return_df_format = True):
        super().__init__()
        self._spi = None
        self._wait_event = wait_event
        self._return_df_format = return_df_format

    def SetSpi(self, spi):
        self._spi = spi

    def OnMDOrderQueue(self, order_queues, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_OrderQueueToJson(order_queues, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)
        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(order_queues)
            if self._wait_event:
                self._wait_event.set()

    def OnStatus(self, status):
        if not self._spi is None:
            self._spi(None, status.error_code) 
        tgw.IGMDApi_FreeMemory(status)
        if self._wait_event:
            self._wait_event.set() 


class TmpQueryOrderQueueWaitSpi(object):
    """
    功能描述：查询委托队列spi，实现同步查询接口的基础，配合TmpQueryOrderQueueSpi实现同步查询
    """
    def __init__(self):
        super().__init__()
        self._result = None
        self._err = tgw.ErrorCode.kSuccess
    def GetResult(self):
        return self._result, self._err

    def OnResponse(self, result, status):
        if not result is None:
            self._result = result
        if not status is None:
            if  isinstance(status, str):
                self._err = status
            elif  isinstance(status, int):
                self._err = status


class TmpQueryTickExecutionSpi(tgw.IGMDTickExecutionSpi):
    """
    功能描述：查询逐笔成交spi，中间层spi，充当适配器。配合TmpQueryTickExecutionWaitSpi对象使用
    """
    def __init__(self, wait_event = None, return_df_format = True):
        super().__init__()
        self._spi = None
        self._wait_event = wait_event
        self._return_df_format = return_df_format

    def SetSpi(self, spi):
        self._spi = spi

    def OnMDTickExecution(self, tick_execution, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_TickExecutionToJson(tick_execution, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)
        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(tick_execution)
            if self._wait_event:
                self._wait_event.set()

    def OnStatus(self, status):
        if not self._spi is None:
            self._spi(None, status.error_code) 
        tgw.IGMDApi_FreeMemory(status)
        if self._wait_event:
            self._wait_event.set() 


class TmpQueryTickExecutionWaitSpi(object):
    """
    功能描述：查询委托队列spi，实现同步查询接口的基础，配合TmpQueryTickExecutionSpi实现同步查询
    """
    def __init__(self):
        super().__init__()
        self._result = None
        self._err = tgw.ErrorCode.kSuccess
    def GetResult(self):
        return self._result, self._err

    def OnResponse(self, result, status):
        if not result is None:
            self._result = result
        if not status is None:
            if  isinstance(status, str):
                self._err = status
            elif  isinstance(status, int):
                self._err = status

class TmpQueryTickOrderSpi(tgw.IGMDTickOrderSpi):
    """
    功能描述：查询逐笔委托spi，中间层spi，充当适配器。配合TmpQueryTickOrderWaitSpi对象使用
    """
    def __init__(self, wait_event = None, return_df_format = True):
        super().__init__()
        self._spi = None
        self._wait_event = wait_event
        self._return_df_format = return_df_format

    def SetSpi(self, spi):
        self._spi = spi

    def OnMDTickOrder(self, tick_orders, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_TickOrderToJson(tick_orders, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)
        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(tick_orders)
            if self._wait_event:
                self._wait_event.set()

    def OnStatus(self, status):
        if not self._spi is None:
            self._spi(None, status.error_code)       
        tgw.IGMDApi_FreeMemory(status)
        if self._wait_event:
            self._wait_event.set() 


class TmpQueryTickOrderWaitSpi(object):
    """
    功能描述：查询逐笔委托spi，实现同步查询接口的基础，配合TmpQueryTickOrderSpi实现同步查询
    """
    def __init__(self):
        super().__init__()
        self._result = None
        self._err = tgw.ErrorCode.kSuccess
    def GetResult(self):
        return self._result, self._err

    def OnResponse(self, result, status):
        if not result is None:
            self._result = result
        if not status is None:
            if  isinstance(status, str):
                self._err = status
            elif  isinstance(status, int):
                self._err = status


class TmpQueryCodeTableSpi(tgw.IGMDCodeTableSpi):
    """
    功能描述：查询代码表spi，中间层spi，充当适配器。配合TmpQueryCodeTableWaitSpi对象使用
    """
    def __init__(self, wait_event = None, return_df_format = True):
        super().__init__()
        self._spi = None
        self._wait_event = wait_event
        self._return_df_format = return_df_format

    def SetSpi(self, spi):
        self._spi = spi

    def OnMDCodeTable(self, code_tables, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_CodeTableToJson(code_tables, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)
        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(code_tables)
            if self._wait_event:
                self._wait_event.set()

    def OnStatus(self, status):
        if not self._spi is None:
            self._spi(None, status.error_code)        
        tgw.IGMDApi_FreeMemory(status)
        if self._wait_event:
            self._wait_event.set() 


class TmpQueryCodeTableWaitSpi(object):
    """
    功能描述：查询代码表spi，实现同步查询接口的基础，配合TmpQueryCodeTableSpi实现同步查询
    """
    def __init__(self):
        super().__init__()
        self._result = None
        self._err = tgw.ErrorCode.kSuccess
    def GetResult(self):
        return self._result, self._err

    def OnResponse(self, result, status):
        if not result is None:
            self._result = result
        if not status is None:
            if  isinstance(status, str):
                self._err = status
            elif  isinstance(status, int):
                self._err = status

class TmpQuerySecuritiesInfoSpi(tgw.IGMDSecuritiesInfoSpi):
    """
    功能描述：查询证券信息spi，中间层spi，充当适配器。配合TmpQuerySecuritiesInfoWaitSpi对象使用
    """
    def __init__(self, wait_event = None, return_df_format = True):
        super().__init__()
        self._spi = None
        self._wait_event = wait_event
        self._return_df_format = return_df_format

    def SetSpi(self, spi):
        self._spi = spi

    def OnMDSecuritiesInfo(self, code_tables, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_CodeTableRecordToJson(code_tables, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)
        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(code_tables)
            if self._wait_event:
                self._wait_event.set()

    def OnStatus(self, status):
        if not self._spi is None:
            self._spi(None, status.error_code) 
        tgw.IGMDApi_FreeMemory(status)
        if self._wait_event:
            self._wait_event.set() 


class TmpQuerySecuritiesInfoWaitSpi(object):
    """
    功能描述：查询证券信息spi，实现同步查询接口的基础，配合TmpQuerySecuritiesInfoSpi实现同步查询
    """
    def __init__(self):
        super().__init__()
        self._result = None
        self._err = tgw.ErrorCode.kSuccess
    def GetResult(self):
        return self._result, self._err

    def OnResponse(self, result, status):
        if not result is None:
            self._result = result
        if not status is None:
            if  isinstance(status, str):
                self._err = status
            elif  isinstance(status, int):
                self._err = status


class TmpQueryETFInfoSpi(tgw.IGMDETFInfoSpi):
    """
    功能描述：查询ETF信息spi，中间层spi，充当适配器。配合TmpQuerySecuritiesInfoWaitSpi对象使用
    """
    def __init__(self, wait_event = None, return_df_format = True):
        super().__init__()
        self._spi = None
        self._wait_event = wait_event
        self._return_df_format = return_df_format

    def SetSpi(self, spi):
        self._spi = spi

    def OnMDETFInfo(self, etf_info, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_ETFInfoToJson(etf_info, cnt)
                json_list = json.loads(json_str)
                result = []
                if self._return_df_format:
                    for js in json_list:
                        basic_info = js['basic_info']
                        constituent_stock_info = js['constituent_stock_info']
                        basic_info_df = json_normalize(basic_info)
                        constituent_stock_info_df = json_normalize(constituent_stock_info)
                        result.append((basic_info_df, constituent_stock_info_df))
                else:
                    for js in json_list:
                        basic_info = js['basic_info']
                        onstituent_stock_info = js['constituent_stock_info']
                        result.append((basic_info, onstituent_stock_info))
                self._spi(result, None)
        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(etf_info)
            if self._wait_event:
                self._wait_event.set()

    def OnStatus(self, status):
        if not self._spi is None:
            self._spi(None, status.error_code) 
        tgw.IGMDApi_FreeMemory(status)
        if self._wait_event:
            self._wait_event.set() 

class TmpQueryETFInfoWaitSpi(object):
    """
    功能描述：查询ETF信息spi，实现同步查询接口的基础，配合TmpQueryETFInfoSpi实现同步查询
    """
    def __init__(self):
        super().__init__()
        self._result = None
        self._err = tgw.ErrorCode.kSuccess
    def GetResult(self):
        return self._result, self._err

    def OnResponse(self, result, status):
        if not result is None:
            self._result = result
        if not status is None:
            if  isinstance(status, str):
                self._err = status
            elif  isinstance(status, int):
                self._err = status


class TmpQueryExFactorSpi(tgw.IGMDExFactorSpi):
    """
    功能描述：查询复权因子spi，中间层spi，充当适配器。配合TmpQueryExFactorWaitSpi对象使用
    """
    def __init__(self, wait_event = None, return_df_format = True):
        super().__init__()
        self._spi = None
        self._wait_event = wait_event
        self._return_df_format = return_df_format

    def SetSpi(self, spi):
        self._spi = spi

    def OnMDExFactor(self, ex_factor_tables, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_ExFactorTableToJson(ex_factor_tables, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)

        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(ex_factor_tables)
            if self._wait_event:
                self._wait_event.set()

    def OnStatus(self, status):
        if not self._spi is None:
            self._spi(None, status.error_code)        
        tgw.IGMDApi_FreeMemory(status)
        if self._wait_event:
            self._wait_event.set() 


class TmpQueryExFactorWaitSpi(object):
    """
    功能描述：查询复权因子信息spi，实现同步查询接口的基础，配合TmpQueryExFactorSpi实现同步查询
    """
    def __init__(self):
        super().__init__()
        self._result = None
        self._err = tgw.ErrorCode.kSuccess
    def GetResult(self):
        return self._result, self._err

    def OnResponse(self, result, status):
        if not result is None:
            self._result = result
        if not status is None:
            if  isinstance(status, str):
                self._err = status
            elif  isinstance(status, int):
                self._err = status


class TmpQueryFactorSpi(tgw.IGMDFactorSpi):
    """
    功能描述：查询因子spi，中间层spi，充当适配器。配合TmpQueryFactorWaitSpi对象使用
    """
    def __init__(self, wait_event = None, return_df_format = True):
        super().__init__()
        self._spi = None
        self._wait_event = wait_event
        self._return_df_format = return_df_format

    def SetSpi(self, spi):
        self._spi = spi

    def OnFactor(self, factors, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_FactorToJson(factors, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)
        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(factors)
            if self._wait_event:
                self._wait_event.set()

    def OnStatus(self, status):
        if not self._spi is None:
            self._spi(None, status.error_code)        
        tgw.IGMDApi_FreeMemory(status)
        if self._wait_event:
            self._wait_event.set()


class TmpQueryFactorWaitSpi(object):
    """
    功能描述：查询因子信息spi，实现同步查询接口的基础，配合TmpQueryFactorSpi实现同步查询
    """
    def __init__(self):
        super().__init__()
        self._result = None
        self._err = tgw.ErrorCode.kSuccess
    def GetResult(self):
        return self._result, self._err


    def OnResponse(self, result, status):
        if not result is None:
            self._result = result
        if not status is None:
            if  isinstance(status, str):
                self._err = status
            elif  isinstance(status, int):
                self._err = status


class TmpQueryThirdInfoSpi(tgw.IGMDThirdInfoSpi):
    """
    功能描述：查询三方信息spi，中间层spi，充当适配器。配合TmpQueryThirdInfoWaitSpi对象使用
    """
    def __init__(self, wait_event = None, return_df_format = True):
        super().__init__()
        self._spi = None
        self._wait_event = wait_event
        self._return_df_format = return_df_format

    def SetSpi(self, spi):
        self._spi = spi

    def OnThirdInfo(self, third_info, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_ThirdInfoToJson(third_info, cnt)
                json_list_tmp = json.loads(json_str)
                json_list = []
                for js in json_list_tmp:
                    json_list += js['body']['data']
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(result, None)
        except Exception as e:
            if not self._spi is None:
                self._spi(None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(third_info)
            if self._wait_event:
                self._wait_event.set()

    def OnStatus(self, status):
        if not self._spi is None:
            self._spi(None, status.error_code)
        tgw.IGMDApi_FreeMemory(status)
        if self._wait_event:
            self._wait_event.set() 


class TmpQueryThirdInfoWaitSpi(object):
    """
    功能描述：查询三方信息spi，实现同步查询接口的基础，配合TmpQueryFactorSpi实现同步查询
    """
    def __init__(self):
        super().__init__()
        self._result = None
        self._err = tgw.ErrorCode.kSuccess
    def GetResult(self):
        return self._result, self._err

    def OnResponse(self, result, status):
        if not result is None:
            self._result = result
        if not status is None:
            if  isinstance(status, str):
                self._err = status
            elif  isinstance(status, int):
                self._err = status

class TmpReplaySpi(tgw.IGMDHistorySpi):
    """
    功能描述：回放spi
    """
    def __init__(self, return_df_format = True):
        super().__init__()
        self._spi = None
        self._return_df_format = return_df_format

    def SetSpi(self, spi):
        self._spi = spi
    def OnMDSnapshot(self, task_id, snapshots, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_SnapshotL2ToJson(snapshots, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(task_id, result, None)
        except Exception as e:
            if not self._spi is None:
                self._spi(task_id, None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(snapshots)

    def OnMDIndexSnapshot(self, task_id, index_snapshots, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_IndexSnapshotToJson(index_snapshots, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(task_id, result, None)
        except Exception as e:
            if not self._spi is None: 
                self._spi(task_id, None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(index_snapshots)

    def OnMDHKTSnapshot(self, task_id, hkt_snapshots, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_HKTSnapshotToJson(hkt_snapshots, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(task_id, result, None)
        except Exception as e:
            if not self._spi is None:
                self._spi(task_id, None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(hkt_snapshots)
    
    def OnMDOptionSnapshot(self, task_id, opt_snapshots, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_OptionSnapshotToJson(opt_snapshots, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(task_id, result, None)
        except Exception as e:
            if not self._spi is None:
                self._spi(task_id, None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(opt_snapshots)

    def OnMDTickExecution(self, task_id, ticks, cnt):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_TickExecutionToJson(ticks, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(task_id, result, None)
        except Exception as e:
            if not self._spi is None:
                self._spi(task_id, None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(ticks)

    def OnMDKline(self, task_id, klines, cnt, kline_type):
        try:
            if not self._spi is None:
                json_str = tgw.Tools_KLineToJson(klines, cnt)
                json_list = json.loads(json_str)
                if self._return_df_format:
                    result = json_normalize(json_list)
                else:
                    result = json_list
                self._spi(task_id, result, None)
        except Exception as e:
            if not self._spi is None:
                self._spi(task_id, None, str(e))
        finally:
            tgw.IGMDApi_FreeMemory(klines)

    def OnRspTaskStatus(self, task_id, task_status):
        if not self._spi is None:
            self._err = task_status.error_code
            self._spi(task_id, None, self._err)
        tgw.IGMDApi_FreeMemory(task_status)
