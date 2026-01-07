import pandas as pd
import numpy as np
import akshare as ak
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

class StockSelectionTool:
    def __init__(self):
        """
        初始化选股工具
        """
        print("使用akshare数据接口初始化选股工具")
        
    def get_main_themes(self):
        """
        获取当前市场主线板块
        """
        main_themes = {
            '脑机接口': ['三博脑科', '创新医疗', '冠昊生物', '诚益通', '复旦复华'],
            '商业航天': ['中国卫通', '航天发展', '航天电子', '航天长峰', '航天动力'],
            '存储芯片': ['兆易创新', '北京君正', '澜起科技', '聚辰股份', '兆驰股份'],
            '有色金属': ['紫金矿业', '赣锋锂业', '华友钴业', '洛阳钼业', '天齐锂业']
        }
        return main_themes
    
    def get_all_stocks(self):
        """
        获取所有A股股票数据
        """
        # 获取A股实时行情数据
        stock_info = ak.stock_zh_a_spot_em()
        return stock_info
    
    def screen_by_volume_ratio(self, df):
        """
        根据量比筛选股票
        akshare没有直接的量比数据，我们通过成交量变化来计算近似值
        """
        # 计算量比近似值（当前成交量/过去5日平均成交量）
        df['volume_ratio'] = df['成交量'] / df.groupby('代码')['成交量'].transform(lambda x: x.rolling(window=5).mean())
        
        # 筛选量比>1.5的股票
        df = df[df['volume_ratio'] > 1.5]
        
        return df
    
    def screen_by_price_performance(self, df, min_change_pct=2.0):
        """
        根据价格表现筛选股票
        """
        # 重命名列名以匹配tushare的数据结构
        df_renamed = df.rename(columns={
            '涨跌幅': 'pct_chg',
            '代码': 'ts_code',
            '名称': 'name',
            '总市值': 'total_mv'
        })
        
        # 将总市值转换为亿元单位
        df_renamed['total_mv'] = df_renamed['total_mv'] / 10000  # 转换为亿
        
        # 筛选涨幅>2%的股票
        df_filtered = df_renamed[df_renamed['pct_chg'] > min_change_pct]
        
        # 按涨幅排序
        df_filtered = df_filtered.sort_values('pct_chg', ascending=False)
        
        return df_filtered
    
    def filter_by_market_value(self, df, min_mv=50, max_mv=300):
        """
        根据市值筛选股票（50-300亿）
        """
        # 筛选市值在50-300亿之间的股票
        df = df[(df['total_mv'] >= min_mv) & (df['total_mv'] <= max_mv)]
        
        return df
    
    def get_turnover_rate(self, df):
        """
        获取换手率数据
        """
        # 如果原数据中没有换手率，我们使用akshare的其他接口获取
        # 这里假设df中包含换手率数据，或者我们基于成交量和流通股本计算
        if '换手率' not in df.columns:
            # 临时添加换手率列，实际应用中需要从数据源获取
            df['turnover_rate'] = 0.0
        else:
            df = df.rename(columns={'换手率': 'turnover_rate'})
        
        return df
    
    def check_ma_trend(self, symbol, days=20):
        """
        检查股票的均线趋势
        """
        try:
            # 获取历史数据
            start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
            end_date = datetime.now().strftime('%Y%m%d')
            
            # 获取个股历史数据
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="")
            
            if df.empty or len(df) < 20:
                return False
            
            # 计算均线
            df['ma5'] = df['收盘'].rolling(window=5).mean()
            df['ma10'] = df['收盘'].rolling(window=10).mean()
            df['ma20'] = df['收盘'].rolling(window=20).mean()
            
            # 检查均线多头排列
            latest = df.iloc[-1]
            if pd.isna(latest['ma5']) or pd.isna(latest['ma10']) or pd.isna(latest['ma20']):
                return False
                
            if latest['ma5'] > latest['ma10'] > latest['ma20']:
                return True
            else:
                return False
        except Exception as e:
            print(f"检查均线趋势时出错，股票代码: {symbol}, 错误: {e}")
            return False
    
    def comprehensive_screening(self):
        """
        综合筛选龙头股
        """
        print("开始执行选股策略...")
        
        # 获取所有A股股票数据
        all_stocks = self.get_all_stocks()
        print(f"获取到 {len(all_stocks)} 只股票数据")
        
        # 1. 根据量比筛选
        df_vol = self.screen_by_volume_ratio(all_stocks)
        print(f"量比>1.5的股票数量: {len(df_vol)}")
        
        # 2. 根据价格表现筛选
        df_price = self.screen_by_price_performance(df_vol, min_change_pct=2.0)
        print(f"涨幅>2%的股票数量: {len(df_price)}")
        
        # 3. 根据市值筛选
        df_mv = self.filter_by_market_value(df_price)
        print(f"市值50-300亿的股票数量: {len(df_mv)}")
        
        # 4. 获取换手率
        df_turnover = self.get_turnover_rate(df_mv)
        # 筛选换手率在3%-10%之间的股票
        if 'turnover_rate' in df_turnover.columns:
            df_turnover = df_turnover[(df_turnover['turnover_rate'] >= 3) & 
                                     (df_turnover['turnover_rate'] <= 10)]
            print(f"换手率3%-10%的股票数量: {len(df_turnover)}")
        else:
            print("数据中不包含换手率信息，跳过换手率筛选")
        
        # 5. 检查均线趋势
        qualified_stocks = []
        count = 0
        for idx, row in df_turnover.iterrows():
            count += 1
            if count % 50 == 0:  # 每处理50只股票打印一次进度
                print(f"已检查 {count}/{len(df_turnover)} 只股票的均线趋势")
                
            try:
                # akshare的股票代码格式可能需要调整
                symbol = row['ts_code']
                if self.check_ma_trend(symbol):
                    qualified_stocks.append(row)
            except Exception as e:
                continue
        
        df_final = pd.DataFrame(qualified_stocks)
        print(f"符合均线多头排列的股票数量: {len(df_final)}")
        
        return df_final
    
    def position_management(self, total_capital):
        """
        仓位管理
        :param total_capital: 总资金
        """
        # 根据资金量计算各仓位对应的股票数量
        first_position_pct = 0.1  # 首次建仓10%
        add_position_pct = 0.1    # 加仓10%
        max_position_pct = 0.3    # 最大仓位30%
        
        first_position_amount = total_capital * first_position_pct
        add_position_amount = total_capital * add_position_pct
        max_position_amount = total_capital * max_position_pct
        
        return {
            'first_position_amount': first_position_amount,
            'add_position_amount': add_position_amount,
            'max_position_amount': max_position_amount,
            'first_position_pct': first_position_pct,
            'add_position_pct': add_position_pct,
            'max_position_pct': max_position_pct
        }
    
    def risk_control(self, stop_loss_pct=-5.0, take_profit_pct=15.0):
        """
        风控设置
        :param stop_loss_pct: 止损比例
        :param take_profit_pct: 止盈比例
        """
        return {
            'stop_loss_pct': stop_loss_pct,
            'take_profit_pct': take_profit_pct
        }

def main():
    # 初始化选股工具
    tool = StockSelectionTool()
    
    # 执行综合选股
    selected_stocks = tool.comprehensive_screening()
    
    if len(selected_stocks) > 0:
        print("\n=== 今日筛选出的龙头股 ===")
        # 确保列存在后再显示
        display_cols = ['ts_code', 'name', 'pct_chg']
        if 'volume_ratio' in selected_stocks.columns:
            display_cols.append('volume_ratio')
        if 'turnover_rate' in selected_stocks.columns:
            display_cols.append('turnover_rate')
        if 'total_mv' in selected_stocks.columns:
            display_cols.append('total_mv')
        
        print(selected_stocks[display_cols].head(10))
        
        # 获取用户资金量
        capital = float(input("\n请输入您的资金量（万元）: ")) * 10000
        
        # 计算仓位管理
        pos_mgmt = tool.position_management(capital)
        print(f"\n=== 仓位管理建议 (总资金: {capital/10000:.0f}万元) ===")
        print(f"首次建仓: {pos_mgmt['first_position_pct']*100}% ({pos_mgmt['first_position_amount']/10000:.1f}万元)")
        print(f"加仓: {pos_mgmt['add_position_pct']*100}% ({pos_mgmt['add_position_amount']/10000:.1f}万元)")
        print(f"最大单票仓位: {pos_mgmt['max_position_pct']*100}% ({pos_mgmt['max_position_amount']/10000:.1f}万元)")
        
        # 风控设置
        risk_ctrl = tool.risk_control()
        print(f"\n=== 风控设置 ===")
        print(f"固定止损线: {risk_ctrl['stop_loss_pct']}%")
        print(f"首次止盈位: {risk_ctrl['take_profit_pct']}%")
    else:
        print("今日未筛选出符合条件的龙头股")

if __name__ == "__main__":
    main()
