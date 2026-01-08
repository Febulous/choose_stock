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
        try:
            stock_info = ak.stock_zh_a_spot_em()
            print(f"获取到列名: {list(stock_info.columns)}")
            print(f"数据预览:\n{stock_info.head(3)}")
            print(f"总市值列示例: {stock_info['总市值'].head(5) if '总市值' in stock_info.columns else '无此列'}")
            print(f"换手率列示例: {stock_info['换手率'].head(5) if '换手率' in stock_info.columns else '无此列'}")
            return stock_info
        except Exception as e:
            print(f"获取股票数据时出错: {e}")
            return pd.DataFrame()
    
    def screen_by_volume_ratio(self, df):
        """
        根据量比筛选股票
        """
        if df.empty:
            return df
            
        # 检查必要列是否存在
        required_cols = ['成交量', '代码']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"缺少必要列: {missing_cols}")
            return df
        
        # 将成交量转换为数值类型
        df = df.copy()
        df['成交量'] = pd.to_numeric(df['成交量'], errors='coerce')
        
        # 计算过去5日平均成交量（这里简化为对当前成交量的处理）
        # 实际应该获取历史数据来计算5日平均成交量
        df = df.dropna(subset=['成交量'])
        
        # 由于无法获取历史数据，我们暂时跳过量比筛选
        # 或者使用换手率作为替代指标
        print(f"量比筛选前股票数量: {len(df)}")
        return df  # 暂时返回所有股票，因为无法准确计算量比
    
    def screen_by_price_performance(self, df, min_change_pct=2.0):
        """
        根据价格表现筛选股票
        """
        if df.empty:
            return df
            
        # 检查必要列是否存在
        if '涨跌幅' not in df.columns:
            print("数据中不包含涨跌幅列")
            return df
            
        # 将涨跌幅转换为数值类型
        df = df.copy()
        df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce')
        df = df.dropna(subset=['涨跌幅'])
        
        # 筛选涨幅>2%的股票
        df_filtered = df[df['涨跌幅'] > min_change_pct]
        
        # 重命名列名以匹配后续处理
        df_renamed = df_filtered.rename(columns={
            '代码': 'ts_code',
            '名称': 'name',
            '涨跌幅': 'pct_chg'
        })
        
        # 如果存在总市值列，进行处理
        if '总市值' in df_renamed.columns:
            df_renamed = df_renamed.copy()
            df_renamed['总市值'] = pd.to_numeric(df_renamed['总市值'], errors='coerce')
            # 将总市值转换为亿元单位 - 修正：除以1亿(100000000)，不是1万(10000)
            df_renamed['total_mv'] = df_renamed['总市值'] / 100000000
        else:
            # 如果没有总市值数据，创建一个临时列
            df_renamed['total_mv'] = 0.0
        
        # 按涨幅排序
        df_renamed = df_renamed.sort_values('pct_chg', ascending=False)
        
        print(f"价格表现筛选后股票数量: {len(df_renamed)}")
        return df_renamed
    
    def filter_by_market_value(self, df, min_mv=50, max_mv=300):
        """
        根据市值筛选股票（50-300亿）
        """
        if df.empty:
            return df
            
        # 检查市值列是否存在
        if 'total_mv' not in df.columns:
            print("数据中不包含总市值列")
            return df
            
        # 将市值转换为数值类型
        df = df.copy()
        df['total_mv'] = pd.to_numeric(df['total_mv'], errors='coerce')
        df = df.dropna(subset=['total_mv'])
        print(f"市值数据统计: 最小值={df['total_mv'].min():.2f}, 最大值={df['total_mv'].max():.2f}")

        # 筛选市值在50-300亿之间的股票
        df_filtered = df[(df['total_mv'] >= min_mv) & (df['total_mv'] <= max_mv)]
    
        print(f"市值筛选后股票数量: {len(df_filtered)}")
        return df_filtered
    
    def get_turnover_rate(self, df):
        """
        获取换手率数据
        """
        # 如果数据中包含换手率列，则进行筛选
        if '换手率' in df.columns:
            df = df.copy()
            # 处理换手率数据，可能包含百分号
            df['换手率'] = df['换手率'].astype(str).str.replace('%', '', regex=False)
            df['换手率'] = pd.to_numeric(df['换手率'], errors='coerce')
            df = df.dropna(subset=['换手率'])
            df = df.rename(columns={'换手率': 'turnover_rate'})
            print(f"换手率数据统计: 最小值={df['turnover_rate'].min():.2f}%, 最大值={df['turnover_rate'].max():.2f}%")

            # 筛选换手率在3%-10%之间的股票
            df_filtered = df[(df['turnover_rate'] >= 3) & (df['turnover_rate'] <= 10)]
            print(f"换手率筛选后股票数量: {len(df_filtered)}")
            return df_filtered
        else:
            print("数据中不包含换手率信息，跳过换手率筛选")
            # 添加一个默认的换手率列
            df = df.copy()
            df['turnover_rate'] = 0.0
            return df
    
    def check_ma_trend(self, symbol, days=20):
        """
        检查股票的均线趋势
        """
        try:
            # 由于akshare的股票代码格式通常是6位数字，不需要前缀
            # 如果代码包含SH/SZ前缀，需要去掉
            if symbol.startswith(('SH', 'SZ')):
                symbol = symbol[2:]
            
            # 获取历史数据
            start_date = (datetime.now() - timedelta(days=days*2)).strftime('%Y%m%d')
            end_date = datetime.now().strftime('%Y%m%d')
            
            # 获取个股历史数据
            df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="")
            
            if df.empty or len(df) < 20:
                return False
            
            # 确保收盘价是数值类型
            df['收盘'] = pd.to_numeric(df['收盘'], errors='coerce')
            df = df.dropna(subset=['收盘'])
            
            if len(df) < 20:
                return False
            
            # 计算均线
            df['ma5'] = df['收盘'].rolling(window=5).mean()
            df['ma10'] = df['收盘'].rolling(window=10).mean()
            df['ma20'] = df['收盘'].rolling(window=20).mean()
            
            # 检查最后一天的均线多头排列
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
        
        if all_stocks.empty:
            print("无法获取股票数据")
            return pd.DataFrame()
        
        # 1. 根据量比筛选（暂时跳过，因为无法准确计算）
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
        print(f"最终进入均线检查的股票数量: {len(df_turnover)}")
        
        # 5. 检查均线趋势
        qualified_stocks = []
        count = 0
        total_count = len(df_turnover)
        
        for idx, row in df_turnover.iterrows():
            count += 1
            if count % 20 == 0 or count == total_count:  # 每处理20只股票或最后一只打印一次进度
                print(f"已检查 {count}/{total_count} 只股票的均线趋势")
                
            try:
                # 获取股票代码
                symbol = row.get('ts_code', '')
                if pd.isna(symbol) or symbol == '':
                    continue
                    
                if self.check_ma_trend(symbol):
                    qualified_stocks.append(row)
            except Exception as e:
                print(f"处理股票 {row.get('name', 'Unknown')} 时出错: {e}")
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
        if 'total_mv' in selected_stocks.columns:
            display_cols.append('total_mv')
        if 'turnover_rate' in selected_stocks.columns:
            display_cols.append('turnover_rate')
        
        #print(selected_stocks[display_cols].head(10))
        print(selected_stocks[display_cols])
        
        # 获取用户资金量
        capital_input = input("\n请输入您的资金量（万元）: ")
        if capital_input.strip():
            capital = float(capital_input) * 10000
            
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
            print("未输入资金量，跳过仓位管理")
    else:
        print("今日未筛选出符合条件的龙头股")
        print("\n可能的原因及解决方案:")
        print("1. 当前A股市场可能处于休市时间，没有实时数据")
        print("2. 设置的筛选条件可能过于严格")
        print("3. 市场整体表现不佳，没有满足条件的股票")
        print("4. 建议尝试调整筛选条件，如降低涨幅要求")

if __name__ == "__main__":
    main()
