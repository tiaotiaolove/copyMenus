# coding:utf-8
import pymysql
import ConfigParser
import time
config = ConfigParser.ConfigParser()
config.readfp(open('./config.ini'))
# 获取源数据库的链接参数
ori_host = config.get('ORIGIN', 'host')
ori_port = int(config.get('ORIGIN', 'port'))
ori_user = config.get('ORIGIN', 'user')
ori_password = config.get('ORIGIN', 'password')
ori_database = config.get('ORIGIN', 'database')
ori_charset = config.get('ORIGIN', 'charset')


# 连接源数据库
def connect_origin():
    return pymysql.connect(host=ori_host,
                           port=ori_port,
                           user=ori_user,
                           password=ori_password,
                           database=ori_database,
                           charset=ori_charset)


# 连接目标数据库(可能是多个)
def connect_target(host, port, user, password, database, charset):
    return pymysql.connect(host=host,
                           port=port,
                           user=user,
                           password=password,
                           database=database,
                           charset=charset)


# 从源数据库复制菜单表到目标数据库
def copy_menu_info(host, port, user, password, database, charset):
    origin_con = connect_origin()
    origin_cur = origin_con.cursor()
    origin_cur.execute("SELECT * FROM menu_info")
    rows = origin_cur.fetchall()
    target_con = connect_target(host, port, user, password, database, charset)
    target_cur = target_con.cursor()
    try:
        target_cur.execute("TRUNCATE menu_info")
        target_con.commit()
        insert_auth_sql = "INSERT INTO menu_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for row in rows:
            target_cur.execute(insert_auth_sql, row)
            target_con.commit()
    except Exception as e:
        target_con.rollback()
        print('table-menu_info copy-error!!!')
        print(e)
    finally:
        origin_cur.close()
        origin_con.close()
        target_cur.close()
        target_con.close()


# 从源数据库复制功能表到目标数据库
def copy_function_info(host, port, user, password, database, charset):
    origin_con = connect_origin()
    origin_cur = origin_con.cursor()
    origin_cur.execute("SELECT * FROM function_info")
    rows = origin_cur.fetchall()
    target_con = connect_target(host, port, user, password, database, charset)
    target_cur = target_con.cursor()
    try:
        target_cur.execute("TRUNCATE function_info")
        target_con.commit()
        insert_auth_sql = "INSERT INTO function_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for row in rows:
            target_cur.execute(insert_auth_sql, row)
            target_con.commit()
    except Exception as e:
        target_con.rollback()
        print('table-function_info copy-error!!!')
        print(e)
    finally:
        origin_cur.close()
        origin_con.close()
        target_cur.close()
        target_con.close()


# 从源数据库复制权限表到目标数据库
def copy_authority(host, port, user, password, database, charset):
    origin_con = connect_origin()
    origin_cur = origin_con.cursor()
    origin_cur.execute("SELECT * FROM authority")
    rows = origin_cur.fetchall()
    target_con = connect_target(host, port, user, password, database, charset)
    target_cur = target_con.cursor()
    try:
        target_cur.execute("TRUNCATE authority")
        target_con.commit()
        insert_auth_sql = "INSERT INTO authority VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for row in rows:
            target_cur.execute(insert_auth_sql, row)
            target_con.commit()
    except Exception as e:
        target_con.rollback()
        print('table-authority copy-error!!!')
        print(e)
    finally:
        origin_cur.close()
        origin_con.close()
        target_cur.close()
        target_con.close()


tar_host_list = config.get('TARGET', 'host').split()
tar_port_list = config.get('TARGET', 'port').split()
tar_user_list = config.get('TARGET', 'user').split()
tar_password_list = config.get('TARGET', 'password').split()
tar_database_list = config.get('TARGET', 'database').split()
tar_charset_list = config.get('TARGET', 'charset').split()
# 对多个目标数据库进行复制方法的调用
for i in range(len(tar_host_list)):
    copy_menu_info(
        tar_host_list[i],
        int(tar_port_list[i]),
        tar_user_list[i],
        tar_password_list[i],
        tar_database_list[i],
        tar_charset_list[i]
    )
    copy_function_info(
        tar_host_list[i],
        int(tar_port_list[i]),
        tar_user_list[i],
        tar_password_list[i],
        tar_database_list[i],
        tar_charset_list[i]
    )
    copy_authority(
        tar_host_list[i],
        int(tar_port_list[i]),
        tar_user_list[i],
        tar_password_list[i],
        tar_database_list[i],
        tar_charset_list[i]
    )
print "Execute successfully, close after 3 seconds"
time.sleep(3)
