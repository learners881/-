#中长线股票北向资金—β策略
# __author__ = 'zfy'

# 导入函数库
import jqdata
from jqlib.technical_analysis import *
import operator
import datetime
import numpy as np
import pandas as pd
from jqdata import finance



'''====================初始化函数====================='''


# 初始化函数，设定基准等等
def initialize(context):
    # 设置沪深300为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格),避免未来函数
    set_option('use_real_price', True)
    # 设置报错等级
    log.set_level('order', 'error')
    # 初始一个存取前一天持仓市值的变量
    g.yes_positions_value = 0
    g.positions_values = 0

    # 可调参数
    g.stock_num = 10#每次买入最大持仓个数10只
    g.money_percentage1 = 0.8  # 北上资金大于20亿资金仓位0.8
    g.money_percentage2 = 0.5  # 北上资金小于20亿大于0资金仓位0.5
    g.money_percentage3 = 0.2  # 北上资金小于0资金仓位0.2
    g.stop_profit = 0.14  #止盈比例
    g.stop_loss = 0.07  #止损比例

'''====================初始化函数====================='''


'''=================================================='''

# 获取外资净买入数据
def get_net_buying(context):
    a = finance.run_query(query(finance.STK_ML_QUOTA).filter(finance.STK_ML_QUOTA.day == context.previous_date,
                                                             finance.STK_ML_QUOTA.link_id == 310001).limit(1))   # 沪港通前一日
    b = finance.run_query(query(finance.STK_ML_QUOTA).filter(finance.STK_ML_QUOTA.day == context.previous_date,
                                                             finance.STK_ML_QUOTA.link_id == 310002).limit(1))  # 深港通前一日
    try:   # 可能会出现港交所停止交易的情况导致当天dataframe为空报错，datafrane为空情况下一律使净买入为0
        a_buy_amount = a['buy_amount'][0]
        a_sell_amount = a['sell_amount'][0]
        b_buy_amount = b['buy_amount'][0]
        b_sell_amount = b['sell_amount'][0]
        net_buying =(a_buy_amount-a_sell_amount)+(b_buy_amount-b_sell_amount)
    except:
        net_buying = 0
    return net_buying

# 获得北上资金净买入排行
def get_north_money_50(context):
    stock_list = [] # 存所有沪深港通个股
    stock_dict = {}  # 构建一个字典，key为个股代码，value为个股外资净流入值
    sort_list = [] #筛选后五十个个股
    dates = jqdata.get_trade_days(end_date=context.current_dt, count=3)  # 获取近几个交易日
    df=finance.run_query(query(finance.STK_EL_CONST_CHANGE).filter(finance.STK_EL_CONST_CHANGE.link_id==310001,).order_by(finance.STK_EL_CONST_CHANGE.change_date))
    stock_list = df['code'].tolist()
    df=finance.run_query(query(finance.STK_EL_CONST_CHANGE).filter(finance.STK_EL_CONST_CHANGE.link_id==310002,).order_by(finance.STK_EL_CONST_CHANGE.change_date))
    stock_list = stock_list + df['code'].tolist()
    d1 = np.empty(shape=[0])
    d2 = np.empty(shape=[0])
    d3 = np.empty(shape=[0])
    a = []
    stock_list = get_market_cap_huge(stock_list)
    for stock in stock_list:
        try:
            df_1 = finance.run_query(query(finance.STK_HK_HOLD_INFO).filter(finance.STK_HK_HOLD_INFO.code == stock,finance.STK_HK_HOLD_INFO.share_number,
                                                                            finance.STK_HK_HOLD_INFO.day == dates[-2])) # 获取上一个交易日的持股数量
            df_2 = finance.run_query(query(finance.STK_HK_HOLD_INFO).filter(finance.STK_HK_HOLD_INFO.code == stock,finance.STK_HK_HOLD_INFO.share_number,
                                                                            finance.STK_HK_HOLD_INFO.day == dates[-3])) # 获取上上个交易日的持股数量
            avg_price = (get_price(stock, end_date=dates[-3], frequency='1d', fields=['avg'], skip_paused=False,
                  fq='pre', count=1))['avg'][0]  #获得上上个交易日的均价
            # if  df_1 is None or df_2 is None or avg_price is None:
            #     continue
            if pd.isnull(int64(df_1['share_number'])[0]) or pd.isnull(int64(df_2['share_number'])[0]):
                continue
            d1 = np.append(d1,[[int64(df_1['share_number'])[0]]])
            d2 = np.append(d2,[[int64(df_2['share_number'])[0]]])
            d3 = np.append(d3,[[avg_price]])
            a.append(stock)
        except:continue
    money_flow = ((d1-d2)*d3).tolist()   #采用矩阵运算加快计算效率
    for stock in a:
        stock_dict[stock] = money_flow[a.index(stock)]   #建立一一对应的字典
    stock_dict = sorted(stock_dict.items(), key=lambda item: item[1], reverse=True)  # 按数值大小降序排列，输出变为列表
    # stock_dict = stock_dict[0:50] #取前五十
    for i in stock_dict:
        if len(sort_list)<50:
            sort_list.append(i[0])
        else:break
    return sort_list


# 财务数据筛选，选出roe大于15%，可加入交易股票池
def analyze_fundmentals(stock_list):
    fundmental_list = []
    # 获得query对象
    for stock in stock_list:
        try:
            q = query(indicator.roe).filter(valuation.code == stock)
            df = get_fundamentals(q)
            df_roe = df.roe[0]
            if df_roe >0.15:
                fundmental_list.append(stock)
        except:
            continue
    return fundmental_list

#对市值大小作降序排列
def sort_market_cap(stock_list):
    sort_list = [] #排序后存的个股代码列表
    stock_dict = {} # 构建一个字典，key为个股代码，value为个股市值
    for stock in stock_list:
        q = query(valuation.market_cap).filter(valuation.code == stock) #获取市值
        df = get_fundamentals(q)
        df_market_cap = df.market_cap[0]
        stock_dict[stock]=df_market_cap

    stock_dict = sorted(stock_dict.items(), key=lambda item: item[1], reverse=True)  #按市值大小降序排列，输出变为列表
    # 取排序的前三十个股，交易池
    for i in stock_dict:
        if len(sort_list)<30:
            sort_list.append(i[0])
    return sort_list
    
# 去除交易池个股市值小于300的
def get_market_cap_huge(stock_list):  
    stocks = []
    for stock in stock_list:
        try:
            q = query(valuation.market_cap).filter(valuation.code == stock) #获取市值
            df = get_fundamentals(q)
            df_market_cap = df.market_cap[0]
            if df_market_cap>300:
                stocks.append(stock)
        except:continue
    return stocks

# 获得均线数据
def get_ma(stk, date, num, Unit,FT, context):
    ma_ = MA(stk, check_date=date, timeperiod=num, unit=Unit,include_now = FT)
    ma__ = list(ma_.values())[0]
    return ma__
    
# obv重构计算公式
def get_obv(stock,date,context): 
    volume_sum = 0
    history_data =get_price(stock, start_date=get_security_info(stock).start_date, end_date=date, frequency='1d', fields=['close','volume'], skip_paused=False, fq='pre')
    close_data=history_data['close'].tolist()
    volume_data = history_data['volume'].tolist()
    today_data =get_bars(stock, 1, unit='1d',fields=['close','volume'],include_now=True, end_dt =date)
    for i in range(len(close_data)-2):
        if close_data[i+1]>close_data[i]:
            volume_sum += volume_data[i+1]
        elif close_data[i+1]<close_data[i]:
            volume_sum -= volume_data[i+1]
        else:continue
    if today_data['close'][-1]>history_data['close'][-2]:
        volume_sum += today_data['volume'][-1]
    elif today_data['close'][-1]<history_data['close'][-2]:
        volume_sum -= today_data['volume'][-1]
    volume_sum = volume_sum/100
    return volume_sum
    
# obv里的20均线
def get_obv_ma(stock,date,context): 
    obv_sum=0
    dates = jqdata.get_trade_days( end_date= date, count=20)
    for i in range(20):
        if i ==19 and date == context.current_dt:
            dat = context.current_dt
        else:
            dat = dates[i]
        obv_sum += get_obv(stock,dat,context)
    return obv_sum/20

'''========================================='''
#生成交易池股票列表
def check_stocks(context):
    g.check_out_list = get_north_money_50(context)
    g.check_out_list = analyze_fundmentals(g.check_out_list)
    g.track_list = sort_market_cap(g.check_out_list)

#开盘前选股
def before_trading_start(context):
    g.net_buying = get_net_buying(context) # 外资流入数据

    check_stocks(context) # 获得股池

    log.info('盯盘交易开始了！')

    g.yes_positions_value = g.positions_values  #得到前一天的持仓市值
    log.info('前一天持仓市值：'+ str(g.yes_positions_value))

    g.flag = 0 #用来决定走有或者没有的两个方向
    if context.portfolio.positions_value >0 :
        g.flag = 1
    log.info('交易股票池')
    log.info(g.track_list)

#盘中时时每分钟进行交易  
def handle_data(context,data):
    hour = context.current_dt.hour
    minu = context.current_dt.minute
    cash = context.portfolio.available_cash
    total = context.portfolio.total_value
    log.info('上一个交易日北向资金净买入额：')
    log.info(g.net_buying)
    if g.flag == 0:  # 这里有个问题，要求里，盘前检查完是否有股票持仓市值后，就不再存在要卖出的股票了
        if g.net_buying>20:
            buy_cash = context.portfolio.total_value * g.money_percentage1
            buying(context,g.track_list,buy_cash)
        elif 0<=g.net_buying<=20:
            buy_cash = context.portfolio.total_value * g.money_percentage2
            buying(context,g.track_list,buy_cash)
        elif g.net_buying<0:
            buy_cash = context.portfolio.total_value * g.money_percentage3
            buying(context,g.track_list,buy_cash)

    elif g.flag == 1:
        selling_all(context) #清仓函数
        selling(context,data) #止盈止损函数
        if g.net_buying>=0 and context.portfolio.positions_value/context.portfolio.total_value<1.0:
            buy_cash=context.portfolio.total_value*1.0-context.portfolio.positions_value
            buying(context,g.track_list,buy_cash)
        elif g.net_buying<0 and context.portfolio.positions_value/context.portfolio.total_value<0.4:
            buy_cash=context.portfolio.total_value*0.4-context.portfolio.positions_value
            buying(context,g.track_list,buy_cash)
    log.info('当前持仓市值：'+str(context.portfolio.positions_value))

#交易函数    
def buying(context,stock_list,buy_cash):
    for stock in stock_list:
        g.buy_cash=buy_cash
        if stock not in context.portfolio.positions:
            MA5_ = get_ma(stock,context.current_dt,5,'1d',False,context)
            MA10_ = get_ma(stock,context.current_dt,10,'1d',False,context)

            if MA5_>MA10_: #前一天已经MA5在MA10之上的就过滤
                g.track_list.remove(stock)
                continue
            MA5 = get_ma(stock,context.current_dt,5,'1d',True,context) # 获取今天和前一天的MA5和MA10，保证前一天MA5在MA10下，今天MA5上穿MA10
            MA10 = get_ma(stock,context.current_dt,10,'1d',True,context)

            obv_ = get_obv(stock,context.previous_date,context) #获得前一天的obv
            obv_ma_ = get_obv_ma(stock,context.previous_date,context) #获取的前一天的obv20均线
            
            if obv_>obv_ma_: #前一天obv就在obv_ma之上的就过滤
                 g.track_list.remove(stock)
                 continue
            obv = get_obv(stock,context.current_dt,context) #获得当前的obv
            obv_ma = get_obv_ma(stock,context.current_dt,context) #获得当前的obv20均线
            #触发买入条件：5日/10日均线金叉和obv指标20日金叉
            if MA5_<MA10_ and MA5>=MA10 and obv_<obv_ma_ and obv>= obv_ma : 
                log.info(context.current_dt)
                log.info('买入！' + stock)
                log.info(buy_cash)
                orders = order_target_value(stock, g.buy_cash/2)#每次买入资金的二分之一
                g.track_list.remove(stock)

# 清仓函数
def selling_all(context):
    g.positions_values = context.portfolio.positions_value  #获得当前持仓市值
    log.info('可用资金'+str(context.subportfolios[0].available_cash))
    log.info('总权益'+str(context.portfolio.total_value))
    log.info('当前市值'+str(g.positions_values))
    #当仓位市值大于90%的时候，总市值在上一个交易日回撤4%或者当天市值回撤4%触发止损，如果仓位市值不大于90%，则总市值在上一个交易日的基础上回撤4%，触发止损
    if g.positions_values/context.portfolio.total_value>=0.9:
        if g.positions_values/(g.positions_values+context.subportfolios[0].available_cash)<0.96 or g.positions_values/g.yes_positions_value<0.96:
            for stock in context.portfolio.positions:
                orders = order_target_value(stock, 0)
    else:
        if g.positions_values/g.yes_positions_value<0.96:
            for stock in context.portfolio.positions:
                orders = order_target_value(stock, 0)
    log.info(context.current_dt)
    
# 止盈止损函数
def selling(context,data):
    positions = context.portfolio.long_positions  # 获得持仓dict
    for position in list(positions.values()): #这里是止盈止损操作
        position_avg_cost = position.avg_cost  #获得持仓成本
        cur_price = position.price #获得最新行情价格

        if cur_price/position_avg_cost<0.93:
            log.info(context.current_dt)
            log.info('止损' + position.security)
            orders = order_target_value(position.security, 0)
        '''
        if cur_price/position_avg_cost>1.6:
            log.info(context.current_dt)
            log.info('止盈' + position.security)
            orders = order_target_value(position.security, 0)
        '''
        
#盘后函数
def after_trading_end(context):
    log.info(context.current_dt)
    g.positions_values = context.portfolio.positions_value  # 盘后把持仓价值赋值给临时变量
