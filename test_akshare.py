# test_akshare.py
import akshare as ak

# 尝试获取一些基本数据
stock_info = ak.stock_info_a_code_name()
print(f"获取到 {len(stock_info)} 条股票数据")

# 列出所有以ak_开头的函数
ak_functions = [name for name in dir(ak) if name.startswith('ak_')]
print(f"发现 {len(ak_functions)} 个AkShare接口函数")
print(ak_functions[:10])  # 打印前10个函数名
