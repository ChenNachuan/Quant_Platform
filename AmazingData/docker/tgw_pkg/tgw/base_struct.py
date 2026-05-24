import tgw
class ReplayCfg(object):
    def __init__(self) -> None:
        self.begin_date = 0 # int
        self.end_date = 0 # int
        self.begin_time = 0 # int
        self.end_time = 0 #int
        self.task_id = 0 # int
        self.req_codes = [] # list of tuple [(market, code), ...] like [101, '000001']
        self.cq_flag = 0
        self.cyc_type = 1
        self.auto_complete = 1
        self.replay_speed = 0
        self.cq_date = 0
        self.cyc_def = 0
        self.qj_flag = 0
        self.md_data_type = 0

class ReqFactorCfg(object):
    def __init__(self) -> None:
        self.task_id = 0
        self.factor_type = ''
        self.factor_sub_type = ''
        self.factor_name = ''
        self.begin_date = 0
        self.end_date = 0
        self.begin_time = 0
        self.end_time = 0
        self.security_code = ''
        self.market = 0
        self.category = 0
        self.count = 0
        self.key1 = ''
        self.key2 = ''

class TGWBaseClass:
    def __str__(self):
        attrs = vars(self)
        attr_list = [f"{k}={v!r}" for k, v in attrs.items()]
        return f"<{self.__class__.__name__}:{', '.join(attr_list)}>"
    
class TGWSnapshotL2(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.level_type = 2
            self.bid_price = []
            self.bid_volume = []
            self.offer_price = []
            self.offer_volume = []
            self.covert(data, index)
        else:
            self.level_type = 2
            self.market_type = 0
            self.security_code = ""
            self.orig_time = 0
            self.trading_phase_code = ""
            self.pre_close_price = 0
            self.open_price = 0
            self.high_price = 0
            self.low_price = 0
            self.last_price = 0
            self.close_price = 0
            self.bid_price = []
            self.bid_volume = []
            self.offer_price = []
            self.offer_volume = []
            self.num_trades = 0
            self.total_volume_trade = 0
            self.total_value_trade = 0
            self.total_bid_volume = 0
            self.total_offer_volume = 0
            self.weighted_avg_bid_price = 0
            self.weighted_avg_offer_price = 0
            self.IOPV = 0
            self.yield_to_maturity = 0
            self.high_limited = 0
            self.low_limited = 0
            self.price_earning_ratio1 = 0
            self.price_earning_ratio2 = 0
            self.change1 = 0
            self.change2 = 0
            self.etf_buy_number = 0
            self.etf_buy_amount = 0
            self.etf_buy_money = 0
            self.etf_sell_number = 0
            self.etf_sell_amount = 0
            self.etf_sell_money = 0

    def covert(self, data, index): 
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type  
        self.security_code = item.security_code  
        self.orig_time = item.orig_time
        self.trading_phase_code = item.trading_phase_code
        self.pre_close_price = item.pre_close_price
        self.open_price = item.open_price
        self.high_price = item.high_price
        self.low_price = item.low_price
        self.last_price = item.last_price
        self.close_price = item.close_price
        for i in range(10):
            self.bid_price.append(tgw.Tools_GetInt64DataByIndex(item.bid_price, i))
            self.bid_volume.append(tgw.Tools_GetInt64DataByIndex(item.bid_volume, i))
            self.offer_price.append(tgw.Tools_GetInt64DataByIndex(item.offer_price, i))
            self.offer_volume.append(tgw.Tools_GetInt64DataByIndex(item.offer_volume, i))
        self.num_trades = item.num_trades
        self.total_volume_trade = item.total_volume_trade
        self.total_value_trade = item.total_value_trade
        self.total_bid_volume = item.total_bid_volume
        self.total_offer_volume = item.total_offer_volume
        self.weighted_avg_bid_price = item.weighted_avg_bid_price
        self.weighted_avg_offer_price = item.weighted_avg_offer_price
        self.IOPV = item.IOPV
        self.yield_to_maturity = item.yield_to_maturity
        self.high_limited = item.high_limited
        self.low_limited = item.low_limited
        self.price_earning_ratio1 = item.price_earning_ratio1
        self.price_earning_ratio2 = item.price_earning_ratio2
        self.change1 = item.change1
        self.change2 = item.change2
        self.etf_buy_number = item.etf_buy_number
        self.etf_buy_amount = item.etf_buy_amount
        self.etf_buy_money = item.etf_buy_money
        self.etf_sell_number = item.etf_sell_number
        self.etf_sell_amount = item.etf_sell_amount
        self.etf_sell_money = item.etf_sell_money

class TGWSnapshotL1(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.level_type = 1
            self.bid_price = []
            self.bid_volume = []
            self.offer_price = []
            self.offer_volume = []
            self.covert(data, index)            
        else:
            self.level_type = 1
            self.market_type = 0 
            self.security_code = ""
            self.variety_category = 0
            self.orig_time = 0
            self.trading_phase_code = ""
            self.pre_close_price = 0
            self.open_price = 0
            self.high_price = 0
            self.low_price = 0
            self.last_price = 0
            self.close_price = 0
            self.bid_price = []
            self.bid_volume = []
            self.offer_price = []
            self.offer_volume = []
            self.num_trades = 0
            self.total_volume_trade = 0
            self.total_value_trade = 0
            self.IOPV = 0
            self.high_limited = 0
            self.low_limited = 0
     
    def covert(self, data, index): 
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type  
        self.security_code = item.security_code
        self.variety_category = item.variety_category
        self.orig_time = item.orig_time
        self.trading_phase_code = item.trading_phase_code
        self.pre_close_price = item.pre_close_price
        self.open_price = item.open_price
        self.high_price = item.high_price
        self.low_price = item.low_price
        self.last_price = item.last_price
        self.close_price = item.close_price
        for i in range(5):
            self.bid_price.append(tgw.Tools_GetInt64DataByIndex(item.bid_price, i))
            self.bid_volume.append(tgw.Tools_GetInt64DataByIndex(item.bid_volume, i))
            self.offer_price.append(tgw.Tools_GetInt64DataByIndex(item.offer_price, i))
            self.offer_volume.append(tgw.Tools_GetInt64DataByIndex(item.offer_volume, i))
        self.num_trades = item.num_trades
        self.total_volume_trade = item.total_volume_trade
        self.total_value_trade = item.total_value_trade
        self.IOPV = item.IOPV
        self.high_limited = item.high_limited
        self.low_limited = item.low_limited

class TGWTickOrder(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.covert(data, index)            
        else:
            self.market_type = 0       
            self.security_code = ""
            self.appl_seq_num = 0
            self.channel_no = 0
            self.order_time = 0
            self.order_price = 0
            self.order_volume = 0
            self.side = ""
            self.order_type = ""
            self.md_stream_id = ""
            self.product_status = ""
            self.orig_order_no = 0
            self.biz_index = 0
            self.traded_order_volume = 0           

    def covert(self, data, index): 
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type       
        self.security_code = item.security_code
        self.appl_seq_num = item.appl_seq_num
        self.channel_no = item.channel_no
        self.order_time = item.order_time
        self.order_price = item.order_price
        self.order_volume = item.order_volume
        self.side = chr(item.side)
        self.order_type = chr(item.order_type)
        self.md_stream_id = item.md_stream_id
        self.product_status = item.product_status
        self.orig_order_no = item.orig_order_no
        self.biz_index = item.biz_index
        self.traded_order_volume = item.traded_order_volume

class TGWTickExecution(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.covert(data, index)            
        else:
            self.market_type = 0
            self.security_code = ""
            self.exec_time = 0
            self.channel_no = 0
            self.appl_seq_num = 0
            self.exec_price = 0
            self.exec_volume = 0
            self.value_trade =0
            self.bid_appl_seq_num = 0
            self.offer_appl_seq_num = 0
            self.side = ""
            self.exec_type = ""
            self.md_stream_id = ""
            self.biz_index = 0
            self.variety_category = 0
            self.cancel_flag = 0

    def covert(self, data, index):
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type
        self.security_code = item.security_code
        self.exec_time = item.exec_time
        self.channel_no = item.channel_no
        self.appl_seq_num = item.appl_seq_num
        self.exec_price = item.exec_price
        self.exec_volume = item.exec_volume
        self.value_trade = item.value_trade
        self.bid_appl_seq_num = item.bid_appl_seq_num
        self.offer_appl_seq_num = item.offer_appl_seq_num
        self.side = chr(item.side)
        self.exec_type = chr(item.exec_type)
        self.md_stream_id = item.md_stream_id
        self.biz_index = item.biz_index
        self.variety_category = item.variety_category
        self.cancel_flag = ord(item.cancel_flag)

class TGWOrderQueue(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.volume = []
            self.covert(data, index)            
        else:
            self.market_type = 0
            self.security_code = ""
            self.order_time = 0
            self.side = ""
            self.order_price = 0
            self.order_volume = 0
            self.num_of_orders = 0
            self.items = 0
            self.volume = []
            self.channel_no = 0
            self.md_stream_id = ""
    def covert(self, data, index):
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type
        self.security_code = item.security_code
        self.order_time = item.order_time
        self.side = chr(item.side)
        self.order_price = item.order_price
        self.order_volume = item.order_volume
        self.num_of_orders = item.num_of_orders
        self.items = item.items
        for i in range(50):
            self.volume.append(tgw.Tools_GetInt64DataByIndex(item.volume, i))
        self.channel_no = item.channel_no
        self.md_stream_id = item.md_stream_id

class TGWIndexSnapshot(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.covert(data, index)            
        else:
            self.market_type = 0
            self.security_code = ""
            self.orig_time = 0
            self.trading_phase_code = ""
            self.pre_close_index = 0
            self.open_index = 0
            self.high_index = 0
            self.low_index = 0
            self.last_index = 0
            self.close_index = 0
            self.total_volume_trade = 0
            self.total_value_trade = 0
            self.variety_category = 0
    def covert(self, data, index):
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type
        self.security_code = item.security_code
        self.orig_time = item.orig_time
        self.trading_phase_code = item.trading_phase_code
        self.pre_close_index = item.pre_close_index
        self.open_index = item.open_index
        self.high_index = item.high_index
        self.low_index = item.low_index
        self.last_index = item.last_index
        self.close_index = item.close_index
        self.total_volume_trade = item.total_volume_trade
        self.total_value_trade = item.total_value_trade
        self.variety_category = item.variety_category

class TGWKLine(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.covert(data, index)            
        else:
            self.market_type = 0
            self.security_code = ""
            self.orig_time = 0
            self.kline_time = 0
            self.open_price = 0
            self.high_price = 0
            self.low_price = 0
            self.close_price = 0
            self.volume_trade = 0
            self.value_trade = 0
            self.variety_category = 0
    def covert(self, data, index):
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type
        self.security_code = item.security_code
        self.orig_time = item.orig_time
        self.kline_time = item.kline_time
        self.open_price = item.open_price
        self.high_price = item.high_price
        self.low_price = item.low_price
        self.close_price = item.close_price
        self.volume_trade = item.volume_trade
        self.value_trade = item.value_trade
        self.variety_category = item.variety_category

class TGWOptionSnapshot(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.bid_price = []
            self.bid_volume = []
            self.offer_price = []
            self.offer_volume = []
            self.covert(data, index)            
        else:
            self.market_type = 0
            self.security_code = ""
            self.orig_time = 0
            self.trading_phase_code = ""
            self.total_long_position = 0
            self.total_volume_trade = 0
            self.total_value_trade = 0
            self.pre_settle_price = 0
            self.pre_close_price = 0
            self.open_price = 0
            self.auction_price = 0
            self.auction_volume = 0
            self.high_price = 0
            self.low_price = 0
            self.last_price = 0
            self.close_price = 0
            self.high_limited = 0
            self.low_limited = 0
            self.bid_price = []
            self.bid_volume = []
            self.offer_price = []
            self.offer_volume = []
            self.settle_price = 0
            self.ref_price = 0
            self.contract_type = ""
            self.expire_date = 0
            self.underlying_security_code = ""
            self.exercise_price = 0
            self.variety_category = 0

    def covert(self, data, index):
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type
        self.security_code = item.security_code
        self.orig_time = item.orig_time
        self.trading_phase_code = item.trading_phase_code
        self.total_long_position = item.total_long_position
        self.total_volume_trade = item.total_volume_trade
        self.total_value_trade = item.total_value_trade
        self.pre_settle_price = item.pre_settle_price
        self.pre_close_price = item.pre_close_price
        self.open_price = item.open_price
        self.auction_price = item.auction_price
        self.auction_volume = item.auction_volume
        self.high_price = item.high_price
        self.low_price = item.low_price
        self.last_price = item.last_price
        self.close_price = item.close_price
        self.high_limited = item.high_limited
        self.low_limited = item.low_limited
        for i in range(5):
            self.bid_price.append(tgw.Tools_GetInt64DataByIndex(item.bid_price, i))
            self.bid_volume.append(tgw.Tools_GetInt64DataByIndex(item.bid_volume, i))
            self.offer_price.append(tgw.Tools_GetInt64DataByIndex(item.offer_price, i))
            self.offer_volume.append(tgw.Tools_GetInt64DataByIndex(item.offer_volume, i))
        self.settle_price = item.settle_price
        self.ref_price = item.ref_price
        self.contract_type = item.contract_type
        self.expire_date = item.expire_date
        self.underlying_security_code = item.underlying_security_code
        self.exercise_price = item.exercise_price
        self.variety_category = item.variety_category

class TGWHKTSnapshot(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.bid_price = []
            self.bid_volume = []
            self.offer_price = []
            self.offer_volume = []
            self.covert(data, index)            
        else:
            self.market_type = 0
            self.security_code = ""
            self.orig_time = 0
            self.trading_phase_code = ""
            self.total_volume_trade = 0
            self.total_value_trade = 0
            self.pre_close_price = 0
            self.nominal_price = 0
            self.high_price = 0
            self.low_price = 0
            self.last_price = 0
            self.bid_price = []
            self.bid_volume = []
            self.offer_price = []
            self.offer_volume = []
            self.ref_price = 0
            self.high_limited = 0
            self.low_limited = 0
            self.bid_price_limit_up = 0
            self.bid_price_limit_down = 0
            self.offer_price_limit_up = 0
            self.offer_price_limit_down = 0 
            self.variety_category = 0
 
    def covert(self, data, index):
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type
        self.security_code = item.security_code
        self.orig_time = item.orig_time
        self.trading_phase_code = item.trading_phase_code
        self.total_volume_trade = item.total_volume_trade
        self.total_value_trade = item.total_value_trade
        self.pre_close_price = item.pre_close_price
        self.nominal_price = item.nominal_price
        self.high_price = item.high_price
        self.low_price = item.low_price
        self.last_price = item.last_price
        for i in range(5):
            self.bid_price.append(tgw.Tools_GetInt64DataByIndex(item.bid_price, i))
            self.bid_volume.append(tgw.Tools_GetInt64DataByIndex(item.bid_volume, i))
            self.offer_price.append(tgw.Tools_GetInt64DataByIndex(item.offer_price, i))
            self.offer_volume.append(tgw.Tools_GetInt64DataByIndex(item.offer_volume, i))
        self.ref_price = item.ref_price
        self.high_limited = item.high_limited
        self.low_limited = item.low_limited
        self.bid_price_limit_up = item.bid_price_limit_up
        self.bid_price_limit_down = item.bid_price_limit_down
        self.offer_price_limit_up = item.offer_price_limit_up
        self.offer_price_limit_down = item.offer_price_limit_down
        self.variety_category = item.variety_category

####################################################### 以下结构暂未使用 #######################################
class TGWAfterHourFixedPriceSnapshot(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.covert(data, index)            
        else:
            self.market_type = 0
            self.security_code = ""
            self.variety_category = 0
            self.orig_time = 0
            self.trading_phase_code = ""
            self.close_price = 0
            self.bid_price = 0
            self.bid_volume = 0
            self.offer_price = 0
            self.offer_volume = 0
            self.pre_close_price = 0
            self.num_trades = 0
            self.total_volume_trade = 0
            self.total_value_trade = 0
    def covert(self, data, index):
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type
        self.security_code = item.security_code
        self.variety_category = item.variety_category
        self.orig_time = item.orig_time
        self.trading_phase_code = item.trading_phase_code
        self.close_price = item.close_price
        self.bid_price = item.bid_price
        self.bid_volume = item.bid_volume
        self.offer_price = item.offer_price
        self.offer_volume = item.offer_volume
        self.pre_close_price = item.pre_close_price
        self.num_trades = item.num_trades
        self.total_volume_trade = item.total_volume_trade
        self.total_value_trade = item.total_value_trade

class TGWCSIIndexSnapshot(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.covert(data, index)            
        else:
            self.market_type = 0
            self.index_market = 0
            self.security_code = ""
            self.orig_time = 0
            self.last_index = 0
            self.open_index = 0
            self.high_index = 0
            self.low_index = 0
            self.close_index = 0
            self.pre_close_index = 0
            self.change = 0
            self.ratio_of_change = 0
            self.total_volume_trade = 0
            self.total_value_trade = 0
            self.exchange_rate = 0
            self.currency_symbol = ""
            self.variety_category = 0
    def covert(self, data, index):
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type
        self.index_market = item.index_market
        self.security_code = item.security_code
        self.orig_time = item.orig_time
        self.last_index = item.last_index
        self.open_index = item.open_index
        self.high_index = item.high_index
        self.low_index = item.low_index
        self.close_index = item.close_index
        self.pre_close_index = item.pre_close_index
        self.change = item.change
        self.ratio_of_change = item.ratio_of_change
        self.total_volume_trade = item.total_volume_trade
        self.total_value_trade = item.total_value_trade
        self.exchange_rate = item.exchange_rate
        self.currency_symbol = item.currency_symbol
        self.variety_category = item.variety_category

class TGWCnIndexSnapshot(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.covert(data, index)            
        else:
            self.market_type = 0
            self.security_code = ""
            self.orig_time = 0
            self.trading_phase_code = ""
            self.pre_close_index = 0
            self.open_index = 0
            self.high_index = 0
            self.low_index = 0
            self.last_index = 0
            self.close_index = 0
            self.total_volume_trade = 0
            self.total_value_trade = 0
            self.variety_category = 0
    def covert(self, data, index):
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type
        self.security_code = item.security_code
        self.orig_time = item.orig_time
        self.trading_phase_code = item.trading_phase_code
        self.pre_close_index = item.pre_close_index
        self.open_index = item.open_index
        self.high_index = item.high_index
        self.low_index = item.low_index
        self.last_index = item.last_index
        self.close_index = item.close_index
        self.total_volume_trade = item.total_volume_trade
        self.total_value_trade = item.total_value_trade
        self.variety_category = item.variety_category

class TGWHKTRealtimeLimit(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.covert(data, index)            
        else:
            self.market_type = 0
            self.orig_time = 0
            self.threshold_amount = 0
            self.pos_amt = 0
            self.amount_status = ""
            self.mkt_status = ""
            self.variety_category = 0
    def covert(self, data, index):
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type
        self.orig_time = item.orig_time
        self.threshold_amount = item.threshold_amount
        self.pos_amt = item.pos_amt
        self.amount_status = item.amount_status
        self.mkt_status = item.mkt_status
        self.variety_category = item.variety_category

class TGWHKTProductStatus(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.covert(data, index)            
        else:
            self.market_type = 0
            self.security_code = ""
            self.orig_time = 0
            self.trading_status1 = ""
            self.trading_status2 = ""
            self.variety_category = 0
    def covert(self, data, index):
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type
        self.security_code = item.security_code
        self.orig_time = item.orig_time
        self.trading_status1 = item.trading_status1
        self.trading_status2 = item.trading_status2
        self.variety_category = item.variety_category

class TGWHKTVCM(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.covert(data, index)            
        else:
            self.market_type = 0
            self.security_code = ""
            self.orig_time = 0
            self.start_time = 0
            self.end_time = 0
            self.ref_price = 0
            self.low_price = 0
            self.high_price = 0
            self.variety_category = 0
    def covert(self, data, index):
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type
        self.security_code = item.security_code
        self.orig_time = item.orig_time
        self.start_time = item.start_time
        self.end_time = item.end_time
        self.ref_price = item.ref_price
        self.low_price = item.low_price
        self.high_price = item.high_price
        self.variety_category = item.variety_category

class TGWFutureSnapshot(TGWBaseClass):
    def __init__(self, data = None, index = None):
        if data is not None and index is not None:
            self.bid_price = []
            self.bid_volume = []
            self.offer_price = []
            self.offer_volume = []
            self.covert(data, index)            
        else:
            self.market_type = 0
            self.security_code = ""
            self.orig_time = 0
            self.action_day = 0
            self.last_price = 0
            self.pre_settle_price = 0
            self.pre_close_price = 0
            self.pre_open_interest = 0
            self.open_price = 0
            self.high_price = 0
            self.low_price = 0
            self.total_volume_trade = 0
            self.total_value_trade = 0
            self.open_interest = 0
            self.close_price = 0
            self.settle_price = 0
            self.high_limited = 0
            self.low_limited = 0
            self.pre_delta = 0
            self.curr_delta = 0
            self.bid_price = []
            self.bid_volume = []
            self.offer_price = []
            self.offer_volume = []
            self.average_price = 0
            self.trading_day = 0
            self.variety_category = 0
            self.latest_volume_trade = 0
            self.init_volume_trade = 0
            self.change_volume_trade = 0
            self.bid_imply_volume = 0
            self.offer_imply_volume = 0
            self.total_bid_volume_trade = 0
            self.total_ask_volume_trade = 0
            self.exchange_inst_id = ""
    def covert(self, data, index):
        item = tgw.Tools_GetDataByIndex(data, index)
        self.market_type = item.market_type
        self.security_code = item.security_code
        self.orig_time = item.orig_time
        self.action_day = item.action_day
        self.last_price = item.last_price
        self.pre_settle_price = item.pre_settle_price
        self.pre_close_price = item.pre_close_price
        self.pre_open_interest = item.pre_open_interest
        self.open_price = item.open_price
        self.high_price = item.high_price
        self.low_price = item.low_price
        self.total_volume_trade = item.total_volume_trade
        self.total_value_trade = item.total_value_trade
        self.open_interest = item.open_interest
        self.close_price = item.close_price
        self.settle_price = item.settle_price
        self.high_limited = item.high_limited
        self.low_limited = item.low_limited
        self.pre_delta = item.pre_delta
        self.curr_delta = item.curr_delta
        for i in range(5):
            self.bid_price.append(tgw.Tools_GetInt64DataByIndex(item.bid_price, i))
            self.bid_volume.append(tgw.Tools_GetInt64DataByIndex(item.bid_volume, i))
            self.offer_price.append(tgw.Tools_GetInt64DataByIndex(item.offer_price, i))
            self.offer_volume.append(tgw.Tools_GetInt64DataByIndex(item.offer_volume, i))
        self.average_price = item.average_price
        self.trading_day = item.trading_day
        self.variety_category = item.variety_category
        self.latest_volume_trade = item.latest_volume_trade
        self.init_volume_trade = item.init_volume_trade
        self.change_volume_trade = item.change_volume_trade
        self.bid_imply_volume = item.bid_imply_volume
        self.offer_imply_volume = item.offer_imply_volume
        self.total_bid_volume_trade = item.total_bid_volume_trade
        self.total_ask_volume_trade = item.total_ask_volume_trade
        self.exchange_inst_id = item.exchange_inst_id



