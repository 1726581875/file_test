import socket
import struct
import sys
import traceback


# 定义要连接的主机和端口
host = 'localhost'
#host = '159.75.134.161'
port = 8888


# 函数：接收网络传输固定长度字节数组
def recv_data(sock, length):
    # 用于接收所有数据的缓冲区
    buffer = b''
    # 已接收的字节数
    received_bytes = 0

    while received_bytes < length:
        # 每次接收剩余的字节数
        data = sock.recv(length - received_bytes)
        if not data:
            # 如果连接关闭或发生错误，退出循环
            break
        buffer += data
        received_bytes += len(data)

    return buffer

# 函数：获取成功包体内容对象
def get_success_packet_content(client_socket, packet_len):
    """
     成功会返回一项格式包内容
     {
       byte opType  --操作类型
       int affRows -- 受影响的行数
       int resRows -- 询结果行数
       byte commandType -- 请求命令类型
       int contentLen -- 内容对象结果长度
       对象 content {...} -- 内容对象bytes
      }
    """
    packet_all = recv_data(client_socket, packet_len)
    # 1 byte opType操作类型（更新还是查询还是删除）, 目前还没使用, 暂时跳过
    # 4 byte affRows受影响的行数(更新操作) , 目前还没使用, 暂时跳过
    # 4 byte resRows查询结果行数(查询操作) , 目前还没使用, 暂时跳过
    # 1 byte commandType 请求命令类型， 就是发送时候传的commandType，不需要解析
    start_index = 1 + 4 + 4 + 1

    # 解析包里对象内容长度
    content_len = struct.unpack('>i', packet_all[start_index: start_index + 4])[0]
    # 对象内容字节数组
    content_start = start_index + 4
    content_obj_arr = packet_all[content_start:content_start + content_len]
    return content_obj_arr

# 函数：打印输出错误结果，并且返回错误信息
def parse_print_err_msg(client_socket, packet_len):
    packet_content = recv_data(client_socket, packet_len)
    err_code = struct.unpack('>i', packet_content[:4])[0]
    msg_len = struct.unpack('>i', packet_content[4:8])[0]
    err_msg = struct.unpack('>{}s'.format(msg_len), packet_content[8:])[0].decode("utf-8")
    print('错误。err_code:' + str(err_code) + ', err_msg:' + err_msg)
    return err_msg

# 函数：获取数据库id函数
def get_database_id(sock, database_name):
    # 0-查询数据库信息
    command_type = 0
    name_bytes = database_name.encode('utf-8')
    byte_len = len(name_bytes)
    # 按大端序序列化
    packed_data = struct.pack('>Bi{}s'.format(byte_len), command_type, byte_len, name_bytes)
    # 发送数据
    client_socket.send(packed_data)

    # 获取返回结果包长度
    packet_len = int.from_bytes(recv_data(client_socket, 4), byteorder='big')
    # 包类型 0:错误 1:成功
    packet_type = recv_data(client_socket, 1)[0]

    database_id = -1
    # 解析结果
    if packet_type == 1:
        content_arr = get_success_packet_content(client_socket, packet_len)
        # 获取数据库id
        database_id = struct.unpack('>i', content_arr[4:8])[0]
    elif packet_type == 0:
        parse_print_err_msg(client_socket, packet_len)
    else:
        print('接收到未知包类型。packet_type:' + str(packet_type))

    return database_id



print('''
  __     __                    _____   ____   _
  \ \   / /                   / ____| / __ \ | |
   \ \_/ /__ _  _ __   _   _ | (___  | |  | || |
    \   // _` || '_ \ | | | | \___ \ | |  | || |
     | || (_| || | | || |_| | ____) || |__| || |____
     |_| \__,_||_| |_| \__, ||_____/  \___\_\|______|
                        __/ |  yanySQL 1.0
                       |___/   author: 摸鱼大师
 ''')
print("请输入命令...")

# 当前数据库id
database_id = -1

# 查询结果格式，0横向表格、1竖向表格
format_type = 0

while True:
    # 创建socket对象
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # 连接服务器
    client_socket.connect((host, port))
    # 接收用户输入
    user_input = input("yanySQL> ")
    user_input = user_input.split(";")[0]
    # 检查用户输入是否为 'exit', exit则结束程序
    if user_input.lower() == 'exit' or user_input.lower() == 'exit;':
        print("程序结束！")
        break
    # 判断是否是切换显示格式，模仿postgres那样使用\x来切换格式
    if user_input == '\\x':
        if format_type == 0:
            format_type = 1
            print("扩展显示已打开.")
        else:
            format_type = 0
            print("扩展显示已关闭.")
        continue
    # 判断是否是use databaseName命令
    if user_input.startswith("use "):
        database_name = user_input.split()[1]
        db_id = get_database_id(client_socket, database_name)
        if db_id == -1:
            print("使用数据库失败！！！！")
        else:
            database_id = db_id
            print("ok")
        continue
    # 校验是否已经使用数据库
    if database_id == -1 and user_input != 'show databases' and user_input != 'SHOW DATABASES':
        print("请先使用use databaseName命令确认当前数据库，再执行查询语句..")
        continue

    # 命令类型：2-执行查询命令，获取格式化查询结果
    command_type = 2
    try:
        # sql转换位字节
        sql_bytes = user_input.encode('utf-8')
        byte_len = len(sql_bytes)
        # 按大端序序列化字节
        packed_data = struct.pack('>Bii{}sB'.format(byte_len), command_type, database_id, byte_len, sql_bytes, format_type)
        # 发送数据
        client_socket.send(packed_data)
        # 获取返回结果
        # 包长度
        packet_len_arr = recv_data(client_socket, 4)
        packet_len = int.from_bytes(packet_len_arr, byteorder='big')

        # 包类型 0:错误 1:成功
        packet_type = recv_data(client_socket, 1)[0]
        # 解析结果
        if packet_type == 1:
            """
             成功会返回以下格式包内容
             {
               byte opType  --操作类型
               int affRows -- 受影响的行数
               int resRows -- 询结果行数
               byte commandType -- 请求命令类型
               int contentLen -- 内容对象结果长度
               对象 content { -- 内容对象
                  int totalByteLen -- 总占用字节数，其实和contentLen一样
                  String resultStr -- 字符串。默认都是[4字节描述长度 + 本身转utf-8字节数组]
                 }
              }
            """
            content_obj_arr = get_success_packet_content(client_socket, packet_len)
            # 字符串长度
            result_str_Len = struct.unpack('>i', content_obj_arr[4: 8])[0]
            # 结果字符串
            result_str = struct.unpack('>{}s'.format(result_str_Len), content_obj_arr[8:])[0].decode("utf-8")
            print("查询结果")
            print(result_str)
        elif packet_type == 0:
            parse_print_err_msg(client_socket, packet_len)
        else:
            print('接收到未知包类型。packet_type:' + str(packet_type))

    except Exception as e:
        print("发生异常", str(e))
        traceback.print_exc()
    finally:
        # 关闭套接字连接
        client_socket.close()