import pymysql

def test_mysql_connection():
    # 数据库配置信息 - 根据你的环境修改
    config = {
        'host': 'localhost',      # 数据库主机地址
        'port': 3306,             # 端口号，默认3306
        'user': 'root',  # 数据库用户名
        'password': 'hzq8822491',  # 数据库密码
        'database': 'akshare_data',    # 要连接的数据库名
        'charset': 'utf8mb4',      # 字符编码
        #'auth_plugin': "mysql_native_password"
    }

    try:
        # 建立数据库连接
        connection = pymysql.connect(**config)
        
        # 创建游标对象
        with connection.cursor() as cursor:
            # 执行简单测试查询
            cursor.execute("SELECT VERSION()")
            db_version = cursor.fetchone()[0]
            print(f"✔ 数据库连接成功！\nMySQL版本: {db_version}")
            
            # 可选：测试数据库名
            cursor.execute("SELECT DATABASE()")
            db_name = cursor.fetchone()[0]
            print(f"当前数据库: {db_name}")
            
        return True
    
    except pymysql.Error as e:
        print(f"✘ 数据库连接失败！错误信息: {e}")
        return False
    
    finally:
        # 确保关闭数据库连接
        if 'connection' in locals() and connection.open:
            connection.close()
            print("数据库连接已关闭")

# 执行测试
if __name__ == "__main__":
    test_mysql_connection()