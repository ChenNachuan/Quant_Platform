import threading
from tgw.tmp_spi import *
from tgw.base_struct import *
import tgw
g_spi = TmpPushSpi()
g_push_spi = None
g_log_spi = None
g_list_query_spi = []
g_list_replay_spi = []
g_api_model = None
def Login(config, api_mode, path = ''):
    """
    功能描述: 登录接口
    + 参数1 config: 登录参数
    + 参数2 api_mode: 模式 （互联网模式或者托管机房模式）
    + 参数3 path: 互联网模式指定证书及相关路径（无特殊用法可不填写，使用默认路径即可）
    + 返回值: True or False。如果登录失败，会在日志spi中体现原因
    异常描述: 无
    """  
    global g_api_model
    g_api_model = api_mode
    if tgw.IGMDApi_Init(g_spi, config, api_mode, path) != tgw.ErrorCode.kSuccess:
        return False
    return True

def GetVersion():
    """
    功能描述: 获取api版本信息
    + 返回值: api版本
    异常描述: 无
    """
    return tgw.IGMDApi_GetVersion()


def GetTaskID():
    """
    功能描述: 获取task id
    + 返回值: task id
    异常描述: 无
    """    
    return tgw.IGMDApi_GetTaskID()

def UpdatePassWord(update_password_req):
    """
    功能描述: 更新密码
    + 返回值: 错误码，0表示无错误。可以通过tgw.GetErrorMsg(error_code) -> str 获取错误信息
    异常描述: 无
    """  
    return tgw.IGMDApi_UpdatePassWord(update_password_req)


def SetLogSpi(log_spi):
    """
    功能描述: 设置
    + 参数1 log_spi: 日志spi
    + 返回值: 无
    异常描述: 无
    """
    global g_spi
    global g_log_spi
    g_log_spi = log_spi
    g_spi.SetLogSpi(log_spi)


#--------------------推送相关接口------------------------

def Subscribe(sub_item, push_spi = None):
    """
    功能描述: 订阅接口
    + 参数1 sub_item: 订阅参数(tgw.SubscribeItem or [tgw.SubscribeItem, tgw.SubscribeItem , ...])
    + 参数2 push_spi: 应用层自己实现的spi
    + 返回值: 错误码，0表示无错误。可以通过tgw.GetErrorMsg(error_code) -> str 获取错误信息
    异常描述: 无
    """
    global g_push_spi
    if not g_push_spi and push_spi:
        g_push_spi = push_spi
        global g_spi
        g_spi.SetSpi(g_push_spi)
    if isinstance(sub_item, list):
        sub_items = tgw.Tools_CreateSubscribeItem(len(sub_item))
        for i, item in enumerate(sub_item):
            tgw.Tools_SetSubscribeItem(sub_items, i, item)
        error_code = tgw.IGMDApi_Subscribe(sub_items, len(sub_item))
        tgw.Tools_DestroySubscribeItem(sub_items)
    else:
       error_code = tgw.IGMDApi_Subscribe(sub_item, 1)
    return error_code

def SubFactor(sub_factor_item, push_spi = None):
    """
    功能描述: 因子订阅接口
    + 参数1 sub_factor_item: 订阅参数 (tgw.SubFactorItem or [tgw.SubFactorItem, tgw.SubFactorItem, ...])
    + 参数2 push_spi: 应用层自己实现的spi
    + 返回值: 错误码，0表示无错误。可以通过tgw.GetErrorMsg(error_code) -> str 获取错误信息
    异常描述: 无
    """
    global g_push_spi
    if not g_push_spi and push_spi:
        g_push_spi = push_spi
        global g_spi
        g_spi.SetSpi(g_push_spi)
    if isinstance(sub_factor_item, list):
        sub_items = tgw.Tools_CreateSubFactorItem(len(sub_factor_item))
        for i, item in enumerate(sub_factor_item):
            tgw.Tools_SetSubFactorItem(sub_items, i, item)
        error_code = tgw.IGMDApi_SubFactor(sub_items, len(sub_factor_item))
        tgw.Tools_DestroySubFactorItem(sub_items)
    else:
       error_code = tgw.IGMDApi_SubFactor(sub_factor_item, 1)        
    return error_code


def UnSubscribe(sub_item, push_spi = None):
    """
    功能描述: 取消订阅
    + 参数1 sub_item: 取消订阅参数(tgw.SubscribeItem or [tgw.SubscribeItem, tgw.SubscribeItem , ...])
    + 参数2 push_spi: 应用层自己实现的spi
    + 返回值: 错误码，0表示无错误。可以通过tgw.GetErrorMsg(error_code) -> str 获取错误信息
    异常描述: 无
    """
    global g_push_spi
    if not g_push_spi and push_spi:
        g_push_spi = push_spi
        global g_spi
        g_spi.SetSpi(g_push_spi)
    if isinstance(sub_item, list):
        sub_items = tgw.Tools_CreateSubscribeItem(len(sub_item))
        for i, item in enumerate(sub_item):
            tgw.Tools_SetSubscribeItem(sub_items, i, item)
        error_code = tgw.IGMDApi_UnSubscribe(sub_items, len(sub_item))
        tgw.Tools_DestroySubscribeItem(sub_items)
    else:
       error_code = tgw.IGMDApi_UnSubscribe(sub_item, 1)
    return error_code

def UnSubFactor(sub_factor_item, push_spi = None):
    """
    功能描述: 取消订阅因子
    + 参数1 sub_factor_item: 取消订阅因子参数 (tgw.SubFactorItem or [tgw.SubFactorItem, tgw.SubFactorItem, ...])
    + 参数2 push_spi: 应用层自己实现的spi
    + 返回值: 错误码，0表示无错误。可以通过tgw.GetErrorMsg(error_code) -> str 获取错误信息
    异常描述: 无
    """
    global g_push_spi
    if not g_push_spi and push_spi:
        g_push_spi = push_spi
        global g_spi
        g_spi.SetSpi(g_push_spi)
    if isinstance(sub_factor_item, list):
        sub_items = tgw.Tools_CreateSubFactorItem(len(sub_factor_item))
        for i, item in enumerate(sub_factor_item):
            tgw.Tools_SetSubFactorItem(sub_items, i, item)
        error_code = tgw.IGMDApi_UnSubFactor(sub_items, len(sub_factor_item))
        tgw.Tools_DestroySubFactorItem(sub_items)
    else:
       error_code = tgw.IGMDApi_UnSubFactor(sub_factor_item, 1)  
    return error_code 


def SubscribeDerivedData(subscribe_type, derived_data_type, derived_data_sub_item, push_spi = None):
    """
    功能描述: 订阅衍生数据
    + 参数1 subscribe_type: 订阅操作类型
    + 参数2 derived_data_type:订阅衍生数据类型
    + 参数3 derived_data_sub_item:衍生数据类型对应的订阅代码数据项
    + 参数4 push_spi: 应用层自己实现的spi
    + 返回值: 错误码，0表示无错误。可以通过tgw.GetErrorMsg(error_code) -> str 获取错误信息
    异常描述: 无
    """
    global g_push_spi
    if not g_push_spi and push_spi:
        g_push_spi = push_spi
        global g_spi
        g_spi.SetSpi(g_push_spi)
    if isinstance(derived_data_sub_item, list):
        sub_items = tgw.Tools_CreateSubscribeDerivedDataItem(len(derived_data_sub_item))
        for i, item in enumerate(derived_data_sub_item):
            tgw.Tools_SetSubscribeDerivedDataItem(sub_items, i, item)
        error_code = tgw.IGMDApi_SubscribeDerivedData(subscribe_type, derived_data_type, sub_items, len(derived_data_sub_item))
        tgw.Tools_DestroySubscribeDerivedDataItem(sub_items)
    else:
       error_code = tgw.IGMDApi_SubscribeDerivedData(subscribe_type, derived_data_type, derived_data_sub_item, 1) 
    return error_code



#------------------查询相关接口-------------------------------

def QueryKline(req_kline_cfg, query_spi = None, return_df_format = True):
    """
    功能描述: 查询k线 托管机房和互联网模式适用
    + 参数1 req_kline_cfg: k线查询结构参数
    + 参数2 query_spi: 用户层回调实现。如果为None，表示同步使用模式。默认为同步模式。如果选择异步模式，需要实现回调函数【OnResponse(result, err_code : int) : result为数据结果，如果为None，可以查看err_code】
    + 参数3 return_df_format: 返回结果的格式。默认为pandas中dataframe格式，如果为False，为json格式
    + 返回值: 一个元组(result,err_code)。result表示为查询结果集。err_code为错误信息。如果结果集为None，可以通过err_code查看相关错误信息。异步模式下，result为True或者False, False的情况下，可以查看err_code。
    异常描述: 无

    使用范围：托管机房和互联网模式适用
    """
    global g_api_model
    err = None
    result = None
    if g_api_model is None:
        err = tgw.ErrorCode.kIllegalMode
    else:
        # 没有回调，说明是同步
        if not query_spi:
            wait_event = threading.Event()
            query_spi = TmpQueryKlineWaitSpi()
            query_kline_spi = TmpQueryKlineSpi(wait_event = wait_event, return_df_format = return_df_format)
            query_kline_spi.SetSpi(query_spi.OnResponse)
            error_code = tgw.IGMDApi_QueryKline(query_kline_spi, req_kline_cfg)
            if error_code != tgw.ErrorCode.kSuccess:
                result = None
                err = error_code
            else:
                wait_event.wait()
                result, err = query_spi.GetResult()
        else:
            query_kline_spi = TmpQueryKlineSpi(return_df_format = return_df_format)
            query_kline_spi.SetSpi(query_spi)
            global g_list_query_spi
            g_list_query_spi.append(query_kline_spi) #保证其生命周期，后续优化释放
            error_code = tgw.IGMDApi_QueryKline(query_kline_spi, req_kline_cfg)
            if error_code != tgw.ErrorCode.kSuccess:
                result = False
                err = error_code
            else:
                result = True
    return result, err


def QuerySnapshot(req_tick_cfg, query_spi = None, return_df_format = True):
    """
    功能描述: 查询快照
    + 参数1 req_tick_cfg: 快照查询结构参数
    + 参数2 query_spi: 用户层回调实现。如果为None，表示同步使用模式。默认为同步模式。如果选择异步模式，需要实现回调函数【OnResponse(result, err_code : int) : result为数据结果，如果为None，可以查看err_code】
    + 参数3 return_df_format: 返回结果的格式。默认为pandas中dataframe格式，如果为False，为json格式
    + 返回值: 一个元组(result,err_code)。result表示为查询结果集。err_code为错误信息。如果结果集为None，可以通过err_code查看相关错误信息。异步模式下，result为True或者False，False的情况下，可以查看err_code
    异常描述: 无

    使用范围：托管机房和互联网模式适用
    """
    global g_api_model
    err = None
    result = None
    if g_api_model is None:
        err = tgw.ErrorCode.kIllegalMode
    # 没有回调，说明是同步
    else:
        if not query_spi:
            wait_event = threading.Event()
            query_spi = TmpQuerySnapshotWaitSpi()
            query_snapshot_spi = TmpQuerySnapshotSpi(wait_event = wait_event, return_df_format = return_df_format)
            query_snapshot_spi.SetSpi(query_spi.OnResponse)
            error_code = tgw.IGMDApi_QuerySnapshot(query_snapshot_spi, req_tick_cfg)
            if error_code != tgw.ErrorCode.kSuccess:
                result = None
                err = error_code
            else:
                wait_event.wait()
                result, err = query_spi.GetResult()
        else:
            query_snapshot_spi = TmpQuerySnapshotSpi(return_df_format = return_df_format)
            query_snapshot_spi.SetSpi(query_spi)
            global g_list_query_spi
            g_list_query_spi.append(query_snapshot_spi) #保证其生命周期，后续优化释放
            error_code = tgw.IGMDApi_QuerySnapshot(query_snapshot_spi, req_tick_cfg)
            if error_code != tgw.ErrorCode.kSuccess:
                result = False
                err = error_code
            else:
                result = True
    return result, err

def QueryOrderQueue(req_order_queue_cfg, query_spi = None, return_df_format = True):
    """
    功能描述: 查询委托队列
    + 参数1 req_order_queue_cfg: 委托队列查询结构参数
    + 参数2 query_spi: 用户层回调实现。如果为None，表示同步使用模式。默认为同步模式。如果选择异步模式，需要实现回调函数【OnResponse(result, err_code : int) : result为数据结果，如果为None，可以查看err_code】
    + 参数3 return_df_format: 返回结果的格式。默认为pandas中dataframe格式，如果为False，为json格式
    + 返回值: 一个元组(result,err_code)。result表示为查询结果集。err_code为错误信息。如果结果集为None，可以通过err_code查看相关错误信息。异步模式下，result为True或者False，False的情况下，可以查看err_code
    异常描述: 无

    使用范围：仅托管机房模式适用
    """
    global g_api_model
    err = None
    result = None
    if g_api_model is None:
        err = tgw.ErrorCode.kIllegalMode
    # 没有回调，说明是同步
    else:
        if not query_spi:
            wait_event = threading.Event()
            query_spi = TmpQueryOrderQueueWaitSpi()
            query_order_queue_spi = TmpQueryOrderQueueSpi(wait_event = wait_event, return_df_format = return_df_format)
            query_order_queue_spi.SetSpi(query_spi.OnResponse)
            error_code = tgw.IGMDApi_QueryOrderQueue(query_order_queue_spi, req_order_queue_cfg)
            if error_code != tgw.ErrorCode.kSuccess:
                result = None
                err = error_code
            else:
                wait_event.wait()
                result, err = query_spi.GetResult()
        else:
            query_order_queue_spi = TmpQueryOrderQueueSpi(return_df_format = return_df_format)
            query_order_queue_spi.SetSpi(query_spi)
            global g_list_query_spi
            g_list_query_spi.append(query_order_queue_spi) #保证其生命周期，后续优化释放
            error_code = tgw.IGMDApi_QueryOrderQueue(query_order_queue_spi, req_order_queue_cfg)
            if error_code != tgw.ErrorCode.kSuccess:
                result = False
                err = error_code
            else:
                result = True
    return result, err


def QueryTickExecution(req_tick_exec_cfg, query_spi = None, return_df_format = True):
    """
    功能描述: 查询逐笔成交
    + 参数1 req_tick_exec_cfg: 逐笔成交查询结构参数
    + 参数2 query_spi: 用户层回调实现。如果为None，表示同步使用模式。默认为同步模式。如果选择异步模式，需要实现回调函数【OnResponse(result, err_code : int) : result为数据结果，如果为None，可以查看err_code】
    + 参数3 return_df_format: 返回结果的格式。默认为pandas中dataframe格式，如果为False，为json格式
    + 返回值: 一个元组(result,err_code)。result表示为查询结果集。err_code为错误信息。如果结果集为None，可以通过err_code查看相关错误信息。异步模式下，result为True或者False，False的情况下，可以查看err_code
    异常描述: 无

    使用范围：仅托管机房模式适用
    """
    global g_api_model
    err = ''
    result = None
    if g_api_model is None:
        err = tgw.ErrorCode.kIllegalMode
    # 没有回调，说明是同步
    else:
        if not query_spi:
            wait_event = threading.Event()
            query_spi = TmpQueryTickExecutionWaitSpi()
            query_tick_exec_spi = TmpQueryTickExecutionSpi(wait_event = wait_event, return_df_format = return_df_format)
            query_tick_exec_spi.SetSpi(query_spi.OnResponse)
            error_code = tgw.IGMDApi_QueryTickExecution(query_tick_exec_spi, req_tick_exec_cfg)
            if error_code != tgw.ErrorCode.kSuccess:
                result = None
                err = error_code
            else:
                wait_event.wait()
                result, err = query_spi.GetResult()
        else:
            query_tick_exec_spi = TmpQueryTickExecutionSpi(return_df_format = return_df_format)
            query_tick_exec_spi.SetSpi(query_spi)
            global g_list_query_spi
            g_list_query_spi.append(query_tick_exec_spi) #保证其生命周期，后续优化释放
            error_code = tgw.IGMDApi_QueryTickExecution(query_tick_exec_spi, req_tick_exec_cfg)
            if error_code != tgw.ErrorCode.kSuccess:
                result = False
                err = error_code
            else:
                result = True
    return result, err    


def QueryTickOrder(req_tick_order_cfg, query_spi = None, return_df_format = True):
    """
    功能描述: 查询逐笔委托
    + 参数1 req_tick_order_cfg: 逐笔委托查询结构参数
    + 参数2 query_spi: 用户层回调实现。如果为None，表示同步使用模式。默认为同步模式。如果选择异步模式，需要实现回调函数【OnResponse(result, err_code : int) : result为数据结果，如果为None，可以查看err_code】
    + 参数3 return_df_format: 返回结果的格式。默认为pandas中dataframe格式，如果为False，为json格式
    + 返回值: 一个元组(result,err_code)。result表示为查询结果集。err_code为错误信息。如果结果集为None，可以通过err_code查看相关错误信息。异步模式下，result为True或者False，False的情况下，可以查看err_code
    异常描述: 无

    使用范围：仅托管机房模式适用
    """
    global g_api_model
    err = None
    result = None
    if g_api_model is None:
        err = tgw.ErrorCode.kIllegalMode
    else :
        # 没有回调，说明是同步
        if not query_spi:
            wait_event = threading.Event()
            query_spi = TmpQueryTickOrderWaitSpi()
            query_tick_order_spi = TmpQueryTickOrderSpi(wait_event = wait_event, return_df_format = return_df_format)
            query_tick_order_spi.SetSpi(query_spi.OnResponse)
            error_code = tgw.IGMDApi_QueryTickOrder(query_tick_order_spi, req_tick_order_cfg)
            if error_code != tgw.ErrorCode.kSuccess:
                result = None
                err = error_code
            else:
                wait_event.wait()
                result, err = query_spi.GetResult()

        else:
            query_tick_order_spi = TmpQueryTickOrderSpi(return_df_format = return_df_format)
            query_tick_order_spi.SetSpi(query_spi)
            global g_list_query_spi
            g_list_query_spi.append(query_tick_order_spi) #保证其生命周期，后续优化释放
            error_code = tgw.IGMDApi_QueryTickOrder(query_tick_order_spi, req_tick_order_cfg)
            if error_code != tgw.ErrorCode.kSuccess:
                result = False
                err = error_code
            else:
                result = True
    return result, err

def QueryCodeTable(query_spi = None, return_df_format = True):
    """
    功能描述: 查询代码表
    + 参数1 query_spi: 用户层回调实现。如果为None，表示同步使用模式。默认为同步模式。如果选择异步模式，需要实现回调函数【OnResponse(result, err_code : int) : result为数据结果，如果为None，可以查看err_code】
    + 参数2 return_df_format: 返回结果的格式。默认为pandas中dataframe格式，如果为False，为json格式
    +返回值: 一个元组(result,err_code)。result表示为查询结果集。err_code为错误信息。如果结果集为None，可以通过err_code查看相关错误信息。异步模式下，result为True或者False，False的情况下，可以查看err_code
    异常描述: 无

    使用范围：托管机房和互联网模式适用
    """
    global g_api_model
    err = None
    result = None
    if g_api_model is None:
        err = tgw.ErrorCode.kIllegalMode
    else :
        # 没有回调，说明是同步
        if not query_spi:
            wait_event = threading.Event()
            query_spi = TmpQueryCodeTableWaitSpi()
            query_code_table_spi = TmpQueryCodeTableSpi(wait_event = wait_event, return_df_format = return_df_format)
            query_code_table_spi.SetSpi(query_spi.OnResponse)
            error_code = tgw.IGMDApi_QueryCodeTable(query_code_table_spi)
            if error_code != tgw.ErrorCode.kSuccess:
                result = None
                err = error_code
            else:
                wait_event.wait()
                result, err = query_spi.GetResult()

        else:
            query_code_table_spi = TmpQueryCodeTableSpi(return_df_format = return_df_format)
            query_code_table_spi.SetSpi(query_spi)
            global g_list_query_spi
            g_list_query_spi.append(query_code_table_spi) #保证其生命周期，后续优化释放
            error_code = tgw.IGMDApi_QueryCodeTable(query_code_table_spi)
            if error_code != tgw.ErrorCode.kSuccess:
                result = False
                err = error_code
            else:
                result = True
    return result, err


def QuerySecuritiesInfo(req_security_info_cfg, query_spi = None, return_df_format = True):
    """
    功能描述: 查询证券信息
    + 参数1 req_security_info_cfg: 证券信息入参
    + 参数2 query_spi: 用户层回调实现。如果为None，表示同步使用模式。默认为同步模式。如果选择异步模式，需要实现回调函数【OnResponse(result, err_code : int) : result为数据结果，如果为None，可以查看err_code】
    + 参数3 return_df_format: 返回结果的格式。默认为pandas中dataframe格式，如果为False，为json格式
    + 返回值: 一个元组(result,err_code)。result表示为查询结果集。err_code为错误信息。如果结果集为None，可以通过err_code查看相关错误信息。异步模式下，result为True或者False，False的情况下，可以查看err_code
    异常描述: 无

    使用范围：托管机房和互联网模式适用
    """
    global g_api_model
    err = None
    result = None
    if g_api_model is None:
        err = tgw.ErrorCode.kIllegalMode
    else :
        # 没有回调，说明是同步
        if not query_spi:
            wait_event = threading.Event()
            query_spi = TmpQuerySecuritiesInfoWaitSpi()
            query_securities_info_spi = TmpQuerySecuritiesInfoSpi(wait_event = wait_event, return_df_format = return_df_format)
            query_securities_info_spi.SetSpi(query_spi.OnResponse)
            if isinstance(req_security_info_cfg, list):
                req_security_info_items = tgw.Tools_CreateSubCodeTableItem(len(req_security_info_cfg))
                for i, item in enumerate(req_security_info_cfg):
                    tgw.Tools_SetSubCodeTableItem(req_security_info_items, i, item)
                error_code = tgw.IGMDApi_QuerySecuritiesInfo(query_securities_info_spi, req_security_info_items, len(req_security_info_cfg))
                tgw.Tools_DestroyCodeTableItem(req_security_info_items)
            else:
                error_code = tgw.IGMDApi_QuerySecuritiesInfo(query_securities_info_spi, req_security_info_cfg, 1)
            if error_code != tgw.ErrorCode.kSuccess:
                result = None
                err = error_code
            else:
                wait_event.wait()
                result, err = query_spi.GetResult()

        else:
            query_securities_info_spi = TmpQuerySecuritiesInfoSpi(return_df_format = return_df_format)
            query_securities_info_spi.SetSpi(query_spi)
            global g_list_query_spi
            g_list_query_spi.append(query_securities_info_spi) #保证其生命周期，后续优化释放
            if isinstance(req_security_info_cfg, list):
                req_security_info_items = tgw.Tools_CreateSubCodeTableItem(len(req_security_info_cfg))
                for i, item in enumerate(req_security_info_cfg):
                    tgw.Tools_SetSubCodeTableItem(req_security_info_items, i, item)
                error_code = tgw.IGMDApi_QuerySecuritiesInfo(query_securities_info_spi, req_security_info_items, len(req_security_info_cfg))
                tgw.Tools_DestroyCodeTableItem(req_security_info_items)
            else:
                error_code = tgw.IGMDApi_QuerySecuritiesInfo(query_securities_info_spi, req_security_info_cfg, 1)
            if error_code != tgw.ErrorCode.kSuccess:
                result = False
                err = error_code
            else:
                result = True
    return result, err

def QueryETFInfo(req_etf_info_cfg, query_spi = None, return_df_format = True):
    """
    功能描述: 查询ETF信息
    + 参数1 req_etf_info_cfg: 查询参数（tgw.tgw.SubCodeTableItem or [tgw.tgw.SubCodeTableItem]）
    + 参数2 query_spi: 用户层回调实现。如果为None，表示同步使用模式。默认为同步模式。如果选择异步模式，需要实现回调函数【OnResponse(result, err_code : int) : result为数据结果，如果为None，可以查看err_code】
    + 参数3 return_df_format: 返回结果的格式。默认为pandas中dataframe格式，如果为False，为json格式
    + 返回值: 一个元组(result,err_code)。result表示为查询结果集。err_code为错误信息。如果结果集为None，可以通过err_code查看相关错误信息。异步模式下，result为True或者False，False的情况下，可以查看err_code。对于dataframe格式，result结果为[(df1,df1'),(df2,df2'),...]，df1为etf信息，df1'为对应成分股信息；对于json格式，result结果为[(json1, [json1', json1'']),... ]，json1为etf信息，json1',json2'' 为对应成分股信息
    异常描述: 无

    使用范围：托管机房和互联网模式适用
    """
    global g_api_model
    err = None
    result = None
    if g_api_model is None:
        err = tgw.ErrorCode.kIllegalMode
    else :
        # 没有回调，说明是同步
        if not query_spi:
            wait_event = threading.Event()
            query_spi = TmpQueryETFInfoWaitSpi()
            query_etf_info_spi = TmpQueryETFInfoSpi(wait_event = wait_event, return_df_format = return_df_format)
            query_etf_info_spi.SetSpi(query_spi.OnResponse)
            if isinstance(req_etf_info_cfg, list):
                req_etf_info_items = tgw.Tools_CreateSubCodeTableItem(len(req_etf_info_cfg))
                for i, item in enumerate(req_etf_info_cfg):
                    tgw.Tools_SetSubCodeTableItem(req_etf_info_items, i, item)
                error_code = tgw.IGMDApi_QueryETFInfo(query_etf_info_spi, req_etf_info_items, len(req_etf_info_cfg))
                tgw.Tools_DestroyCodeTableItem(req_etf_info_items)
            else:
                error_code = tgw.IGMDApi_QueryETFInfo(query_etf_info_spi, req_etf_info_cfg, 1)
            
            if error_code != tgw.ErrorCode.kSuccess:
                result = None
                err = error_code
            else:
                wait_event.wait()
                result, err = query_spi.GetResult()
        else:
            query_etf_info_spi = TmpQueryETFInfoSpi(return_df_format = return_df_format)
            query_etf_info_spi.SetSpi(query_spi)
            global g_list_query_spi
            g_list_query_spi.append(query_etf_info_spi) #保证其生命周期，后续优化释放
            if isinstance(req_etf_info_cfg, list):
                req_etf_info_items = tgw.Tools_CreateSubCodeTableItem(len(req_etf_info_cfg))
                for i, item in enumerate(req_etf_info_cfg):
                    tgw.Tools_SetSubCodeTableItem(req_etf_info_items, i, item)
                error_code = tgw.IGMDApi_QueryETFInfo(query_etf_info_spi, req_etf_info_items, len(req_etf_info_cfg))
                tgw.Tools_DestroyCodeTableItem(req_etf_info_items)
            else:
                error_code = tgw.IGMDApi_QueryETFInfo(query_etf_info_spi, req_etf_info_cfg, 1)
            if error_code != tgw.ErrorCode.kSuccess:
                result = False
                err = error_code
            else:
                result = True
    return result, err


def QueryExFactorTable(security_code, query_spi = None, return_df_format = True):
    """
    功能描述: 查询复权因子
    + 参数1 security_code: 代码
    + 参数2 query_spi: 用户层回调实现。如果为None，表示同步使用模式。默认为同步模式。如果选择异步模式，需要实现回调函数【OnResponse(result, err_code : int) : result为数据结果，如果为None，可以查看err_code】
    + 参数3 return_df_format: 返回结果的格式。默认为pandas中dataframe格式，如果为False，为json格式
    + 返回值: 一个元组(result,err_code)。result表示为查询结果集。err_code为错误信息。如果结果集为None，可以通过err_code查看相关错误信息。异步模式下，result为True或者False，False的情况下，可以查看err_code
    异常描述: 无

    使用范围：托管机房和互联网模式适用
    """
    global g_api_model
    err = None
    result = None
    if g_api_model is None:
        err = tgw.ErrorCode.kIllegalMode

    else :
        # 没有回调，说明是同步
        if not query_spi:
            wait_event = threading.Event()
            query_spi = TmpQueryExFactorWaitSpi()
            query_ex_factor_table_spi = TmpQueryExFactorSpi(wait_event = wait_event, return_df_format = return_df_format)
            query_ex_factor_table_spi.SetSpi(query_spi.OnResponse)
            error_code = tgw.IGMDApi_QueryExFactorTable(query_ex_factor_table_spi, security_code)
            if error_code != tgw.ErrorCode.kSuccess:
                result = None
                err = error_code
            else:
                wait_event.wait()
                result, err = query_spi.GetResult()

        else:
            query_ex_factor_table_spi = TmpQueryExFactorSpi(return_df_format = return_df_format)
            query_ex_factor_table_spi.SetSpi(query_spi)
            global g_list_query_spi
            g_list_query_spi.append(query_ex_factor_table_spi) #保证其生命周期，后续优化释放
            error_code = tgw.IGMDApi_QueryExFactorTable(query_ex_factor_table_spi, security_code)
            if error_code != tgw.ErrorCode.kSuccess:
                result = False
                err = error_code
            else:
                result = True
    return result, err

def QueryFactor(req_factor_cfg, query_spi = None):
    """
    功能描述: 查询因子, 托管机房和互联网模式适用
    + 参数1 req_factor_cfg: 加工因子信息
    + 参数2 query_spi: 用户层回调实现。如果为None，表示同步使用模式。默认为同步模式。如果选择异步模式，需要实现回调函数【OnResponse(result, err_code : int) : result为数据结果，如果为None，可以查看err_code】
    + 返回值: 一个元组(result,err_code)。result表示为查询结果集。err_code为错误信息。如果结果集为None，可以通过err_code查看相关错误信息。result为json数组。异步模式下，result为True或者False，False的情况下，可以查看err_code
    异常描述: 无

    使用范围：托管机房和互联网模式适用
    """
    global g_api_model
    err = None
    result = None

    # ReqFactor结构中，key1， key2是char数组，非必要参数，值为随机值，可能导致上游解析json失败
    # python层多封装一层因子查询参数结构保证key1、key2默认值为空字符串
    # 兼容原来的入参
    if isinstance(req_factor_cfg, tgw.ReqFactorCfg):
        req_factor_cfg_tmp = tgw.ReqFactor()
        req_factor_cfg_tmp.task_id = req_factor_cfg.task_id
        req_factor_cfg_tmp.factor_type = req_factor_cfg.factor_type
        req_factor_cfg_tmp.factor_sub_type = req_factor_cfg.factor_sub_type
        req_factor_cfg_tmp.factor_name = req_factor_cfg.factor_name
        req_factor_cfg_tmp.begin_date = req_factor_cfg.begin_date
        req_factor_cfg_tmp.end_date = req_factor_cfg.end_date
        req_factor_cfg_tmp.begin_time = req_factor_cfg.begin_time
        req_factor_cfg_tmp.end_time = req_factor_cfg.end_time
        req_factor_cfg_tmp.security_code = req_factor_cfg.security_code
        req_factor_cfg_tmp.market = req_factor_cfg.market
        req_factor_cfg_tmp.category = req_factor_cfg.category
        req_factor_cfg_tmp.count = req_factor_cfg.count
        req_factor_cfg_tmp.key1 = req_factor_cfg.key1
        req_factor_cfg_tmp.key2 = req_factor_cfg.key2
    else:
        req_factor_cfg_tmp = req_factor_cfg
    
    if g_api_model is None:
        err = tgw.ErrorCode.kIllegalMode
    else :
        # 没有回调，说明是同步
        if not query_spi:
            wait_event = threading.Event()
            query_spi = TmpQueryFactorWaitSpi()
            query_factor_spi = TmpQueryFactorSpi(wait_event = wait_event, return_df_format = False)
            query_factor_spi.SetSpi(query_spi.OnResponse)
            error_code = tgw.IGMDApi_QueryFactor(query_factor_spi, req_factor_cfg_tmp)
            if error_code != tgw.ErrorCode.kSuccess:
                result = None
                err = error_code
            else:
                wait_event.wait()
                result, err = query_spi.GetResult()

        else:
            query_factor_spi = TmpQueryFactorSpi(return_df_format = False)
            query_factor_spi.SetSpi(query_spi)
            global g_list_query_spi
            g_list_query_spi.append(query_factor_spi) #保证其生命周期，后续优化释放
            error_code = tgw.IGMDApi_QueryFactor(query_factor_spi, req_factor_cfg_tmp)
            if error_code != tgw.ErrorCode.kSuccess:
                result = False
                err = error_code
            else:
                result = True
    return result, err

def QueryHQFactor(req_factor_cfg, query_spi = None):
    if g_api_model is None:
        err = tgw.ErrorCode.kIllegalMode
    else :
        # 没有回调，说明是同步
        if not query_spi:
            wait_event = threading.Event()
            query_spi = TmpQueryFactorWaitSpi()
            query_factor_spi = TmpQueryFactorSpi(wait_event = wait_event, return_df_format = False)
            query_factor_spi.SetSpi(query_spi.OnResponse)
            error_code = tgw.IGMDApi_QueryHQFactor(query_factor_spi, req_factor_cfg)
            if error_code != tgw.ErrorCode.kSuccess:
                result = None
                err = error_code
            else:
                wait_event.wait()
                result, err = query_spi.GetResult()

        else:
            query_factor_spi = TmpQueryFactorSpi(return_df_format = False)
            query_factor_spi.SetSpi(query_spi)
            global g_list_query_spi
            g_list_query_spi.append(query_factor_spi) #保证其生命周期，后续优化释放
            error_code = tgw.IGMDApi_QueryHQFactor(query_factor_spi, req_factor_cfg)
            if error_code != tgw.ErrorCode.kSuccess:
                result = False
                err = error_code
            else:
                result = True
    return result, err

def SetThirdInfoParam(task_id, key, value):
    """
    功能描述: 设置三方资讯查询请求接口 
    + 参数1 task_id: 三方资讯请求编号
    + 参数2 key: 三方资讯请求json的key
    + 参数3 value: 三方资讯请求json的value
    + 返回值: 错误码，0表示无错误。可以通过tgw.GetErrorMsg(error_code) -> str 获取错误信息
    异常描述: 无

    使用范围：托管机房和互联网模式适用
    """
    return tgw.IGMDApi_SetThirdInfoParam(task_id, key, value)

def QueryThirdInfo(task_id, query_spi = None, return_df_format = True):
    """
    功能描述: 查询三方信息
    + 参数1 task_id: 三方资讯请求编号
    + 参数2 query_spi: 用户层回调实现。如果为None，表示同步使用模式。默认为同步模式。如果选择异步模式，需要实现回调函数【OnResponse(result, err_code : int) : result为数据结果，如果为None，可以查看err_code】
    + 参数3 return_df_format: 返回结果的格式。默认为pandas中dataframe格式，如果为False，为json格式
    + 返回值: 一个元组(result,err_code)。result表示为查询结果集。err_code为错误信息。如果结果集为None，可以通过err_code查看相关错误信息。result为json数组。异步模式下，result为True或者False，False的情况下，可以查看err_code
    异常描述: 无

    使用范围：托管机房和互联网模式适用
    """
    global g_api_model
    err = None
    result = None
    if g_api_model is None:
        err = tgw.ErrorCode.kIllegalMode
    else :
        # 没有回调，说明是同步
        if not query_spi:
            wait_event = threading.Event()
            query_spi = TmpQueryThirdInfoWaitSpi()
            query_third_info_spi = TmpQueryThirdInfoSpi(wait_event = wait_event, return_df_format = return_df_format)
            query_third_info_spi.SetSpi(query_spi.OnResponse)
            error_code = tgw.IGMDApi_QueryThirdInfo(query_third_info_spi, task_id)
            if error_code != tgw.ErrorCode.kSuccess:
                result = None
                err = error_code
            else:
                wait_event.wait()
                result, err = query_spi.GetResult()
        else:
            query_third_info_spi = TmpQueryThirdInfoSpi(return_df_format = return_df_format)
            query_third_info_spi.SetSpi(query_spi)
            global g_list_query_spi
            g_list_query_spi.append(query_third_info_spi) #保证其生命周期，后续优化释放
            error_code = tgw.IGMDApi_QueryThirdInfo(query_third_info_spi, task_id)
            if error_code != tgw.ErrorCode.kSuccess:
                result = False
                err = error_code
            else:
                result = True
    return result, err

def ReplayKline(replay_cfg, replay_spi, return_df_format = True):
    """
    功能描述: k线回放。异步接口。
    + 参数1 replay_cfg: 回放参数 (tgw.ReplayCfg)
    + 参数2 replay_spi: 应用层回调函数【OnResponse(task_id, result, err_code : int) : result为数据结果，如果为None，可以查看err_code】。
    + 参数3 return_df_format: 返回的数据格式，默认为dataframe。另外一种是json
    + 返回值: 错误码，0表示无错误。可以通过tgw.GetErrorMsg(err_code) -> str 获取错误信息
    异常描述: 如果参数类型异常，会抛Exception

    使用范围：仅托管机房模式适用
    """
    global g_api_model

    error_code = 0
    if not g_api_model:
        error_code = tgw.ErrorCode.kIllegalMode
        return error_code
    elif g_api_model == tgw.ApiMode.kInternetMode:
        error_code = tgw.ErrorCode.kIllegalMode
        return error_code

    # 参数简单校验
    if not isinstance(replay_cfg.begin_date, int):
        raise Exception("ReplayCfg field begin_date is not int, please check it. It should be like 20211228")

    if not isinstance(replay_cfg.end_date, int):
        raise Exception("ReplayCfg field end_date is not int, please check it. It should be like 20211228")

    if not isinstance(replay_cfg.begin_time, int):
        raise Exception("ReplayCfg field begin_time is not int, please check it. It should be like 930")

    if not isinstance(replay_cfg.end_time, int):
        raise Exception("ReplayCfg field end_time is not int, please check it. It should be like 1031")

    if not isinstance(replay_cfg.task_id, int):
        raise Exception("ReplayCfg field task_id is not int, please check it. It should be created by GetTaskID()")

    if not isinstance(replay_cfg.cq_flag, int):
        raise Exception("ReplayCfg field cq_flag is not int, please check it. It should be like 0")

    if not isinstance(replay_cfg.cyc_type, int):
        raise Exception("ReplayCfg field cyc_type is not int, please check it. It should be like 1")

    if not isinstance(replay_cfg.auto_complete, int):
        raise Exception("ReplayCfg field auto_complete is not int, please check it. It should be like 1")

    if not isinstance(replay_cfg.req_codes, list):
        raise Exception("ReplayCfg field req_codes is not list, please check it. It should be like [(tgw.MarketType.kSZSE, '000001')]")

    for req_code in replay_cfg.req_codes:
        if not isinstance(req_code, tuple):
            raise Exception("item of req_codes is not tuple, please check it. It should be like (tgw.MarketType.kSZSE, '000001')")
        market, code = req_code
        if not isinstance(market, int):
            raise Exception("the first field of req_codes's item  is not int, please check it. It should be like tgw.MarketType.kSZSE")
        if not isinstance(code, str):
            raise Exception("the second field of req_codes's item  is not str, please check it. It should be like '000001'")

    tmp_replay_spi = TmpReplaySpi(return_df_format = return_df_format)
    tmp_replay_spi.SetSpi(replay_spi)
    global g_list_replay_spi
    g_list_replay_spi.append(tmp_replay_spi) #保证其生命周期，后续优化释放

    req_replay = tgw.ReqReplayKline()
    req_replay.begin_date = replay_cfg.begin_date
    req_replay.end_date = replay_cfg.end_date
    req_replay.begin_time = replay_cfg.begin_time
    req_replay.end_time = replay_cfg.end_time
    req_replay.auto_complete = replay_cfg.auto_complete
    req_replay.cq_flag = replay_cfg.cq_flag
    req_replay.cq_date = replay_cfg.cq_date
    req_replay.cyc_def = replay_cfg.cyc_def
    req_replay.qj_flag = replay_cfg.qj_flag
    req_replay.cyc_type = replay_cfg.cyc_type
    req_replay.replay_speed = replay_cfg.replay_speed
    req_replay.task_id =  replay_cfg.task_id
    req_replay.req_item_cnt = len(replay_cfg.req_codes)

    req_replay.req_items = tgw.Tools_CreateReqHistoryItem(req_replay.req_item_cnt)
    for i, item in enumerate(replay_cfg.req_codes):
        req_history_item = tgw.ReqHistoryItem()
        market, code = item
        req_history_item.market = market
        req_history_item.security_code = code
        tgw.Tools_SetReqHistoryItem(req_replay.req_items, i, req_history_item)
    error_code = tgw.IGMDApi_ReplayKline(tmp_replay_spi, req_replay)
    tgw.Tools_DestroyReqHistoryItem(req_replay.req_items)
    return error_code


def ReplayRequest(replay_cfg, replay_spi, return_df_format = True):
    """
    功能描述: 行情回放。异步接口。
    + 参数1 replay_cfg: 回放参数。 (tgw.ReplayCfg)
    + 参数2 replay_spi: 应用层回调函数【OnResponse(task_id, result, err_code : int) : result为数据结果，如果为None，可以查看err_code】。
    + 参数3 return_df_format: 返回的数据格式，默认为dataframe。另外一种是json
    + 返回值: 错误码，0表示无错误。可以通过tgw.GetErrorMsg(err_code) -> str 获取错误信息
    异常描述: 如果参数类型异常，会抛Exception

    使用范围：仅托管机房模式适用
    """
    global g_api_model
    error_code = 0
    if not g_api_model:
        error_code = tgw.ErrorCode.kIllegalMode
        return error_code
    elif g_api_model == tgw.ApiMode.kInternetMode:
        error_code = tgw.ErrorCode.kIllegalMode
        return error_code
    # 参数简单校验
    if not isinstance(replay_cfg.begin_date, int):
        raise Exception("ReplayCfg field begin_date is not int, please check it. It should be like 20211228")

    if not isinstance(replay_cfg.end_date, int):
        raise Exception("ReplayCfg field end_date is not int, please check it. It should be like 20211228")

    if not isinstance(replay_cfg.begin_time, int):
        raise Exception("ReplayCfg field begin_time is not int, please check it. It should be like 91500000")

    if not isinstance(replay_cfg.end_time, int):
        raise Exception("ReplayCfg field end_time is not int, please check it. It should be like 103100000")

    if not isinstance(replay_cfg.md_data_type, int):
        raise Exception("ReplayCfg field md_data_type is not int, please check it. It should be like tgw.MDDatatype.kSnapshot")

    if not isinstance(replay_cfg.task_id, int):
        raise Exception("ReplayCfg field task_id is not int, please check it. It should be created by GetTaskID()")

    if not isinstance(replay_cfg.req_codes, list):
        raise Exception("ReplayCfg field req_codes is not list, please check it. It should be like [(tgw.MarketType.kSZSE, '000001')]")

    for req_code in replay_cfg.req_codes:
        if not isinstance(req_code, tuple):
            raise Exception("item of req_codes is not tuple, please check it. It should be like (tgw.MarketType.kSZSE, '000001')")
        market, code = req_code
        if not isinstance(market, int):
            raise Exception("the first field of req_codes's item  is not int, please check it. It should be like tgw.MarketType.kSZSE")
        if not isinstance(code, str):
            raise Exception("the second field of req_codes's item  is not str, please check it. It should be like '000001'")

    tmp_replay_spi = TmpReplaySpi(return_df_format = return_df_format)
    tmp_replay_spi.SetSpi(replay_spi)
    global g_list_replay_spi
    g_list_replay_spi.append(tmp_replay_spi) #保证其生命周期，后续优化释放
    req_replay = tgw.ReqReplay()
    req_replay.begin_date = replay_cfg.begin_date
    req_replay.end_date = replay_cfg.end_date
    req_replay.begin_time = replay_cfg.begin_time
    req_replay.end_time = replay_cfg.end_time
    req_replay.md_data_type = replay_cfg.md_data_type
    req_replay.task_id =  replay_cfg.task_id
    req_replay.req_item_cnt = len(replay_cfg.req_codes)
    req_replay.req_items = tgw.Tools_CreateReqHistoryItem(req_replay.req_item_cnt)
    for i, item in enumerate(replay_cfg.req_codes):
        req_history_item = tgw.ReqHistoryItem()
        market, code = item
        req_history_item.market = market
        req_history_item.security_code = code
        tgw.Tools_SetReqHistoryItem(req_replay.req_items, i, req_history_item)
    error_code = tgw.IGMDApi_ReplayRequest(tmp_replay_spi, req_replay)
    tgw.Tools_DestroyReqHistoryItem(req_replay.req_items)
    return error_code


def CancelTask(task_id):
    """
    功能描述: 取消回放任务, 仅托管机房
    + 参数1 task_id: 回放id
    + 返回值: 错误码，0表示无错误。可以通过tgw.GetErrorMsg(error_code) -> str 获取错误信息
    异常描述: 无

    
    """
    return tgw.IGMDApi_CancelTask(task_id)

def Close():
    tgw.IGMDApi_Release()
