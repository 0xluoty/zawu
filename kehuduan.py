import socket
import os
import struct

SERVER_HOST = "103.212.187.100"
SERVER_PORT = 8888
BUFFER_SIZE = 4096

def read_msg(sock):
    header = sock.recv(4)
    if len(header) !=4:
        raise ConnectionError("连接断开")
    msg_len = struct.unpack(">I", header)[0]
    data = b""
    while len(data) < msg_len:
        chunk = sock.recv(msg_len - len(data))
        if not chunk:
            raise ConnectionError("读取消息中断")
        data += chunk
    return data

def send_msg(sock, data:bytes):
    length = struct.pack(">I", len(data))
    sock.sendall(length + data)

def send_file(sock, filepath):
    if not os.path.exists(filepath):
        print("[客户端] 文件不存在！")
        return
    filename = os.path.basename(filepath)
    filesize = os.path.getsize(filepath)
    info = f"{filename}|{filesize}".encode()
    send_msg(sock, info)
    resp = read_msg(sock)
    if resp != b"READY":
        print("[客户端] 服务端未就绪，终止上传")
        return
    print(f"[客户端] 开始上传 {filename}")
    with open(filepath, "rb") as f:
        while chunk := f.read(BUFFER_SIZE):
            sock.sendall(chunk)
    print(f"[客户端] ✅ 上传完成")

def receive_file(sock):
    info = read_msg(sock).decode("utf-8")
    filename, filesize_str = info.split("|")
    if filename == "ERROR":
        print(f"[客户端] 服务端错误：{filesize_str}")
        return
    filesize = int(filesize_str)
    print(f"[客户端] 准备接收文件 {filename} 大小 {filesize} bytes")
    send_msg(sock, b"READY")

    save_dir = "client_recv"
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, filename)
    received = 0
    with open(save_path, "wb") as f:
        while received < filesize:
            chunk = sock.recv(min(BUFFER_SIZE, filesize-received))
            if not chunk:
                raise ConnectionError("文件接收中断")
            f.write(chunk)
            received += len(chunk)
            print(f"[客户端] 接收进度 {received}/{filesize}")
    print(f"[客户端] ✅ 文件保存到 {save_path}")

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((SERVER_HOST, SERVER_PORT))
        print("✅ 成功连接服务端")
    except Exception as e:
        print(f"❌ 连接失败: {e}")
        print("检查：安全组/防火墙 888端口TCP是否放行")
        return
    print("====命令====")
    print("send 本地文件路径  → 上传到服务端")
    print("get 服务端文件路径 → 从服务端下载")
    print("exit → 退出")
    while True:
        cmd = input(">>> ")
        send_msg(sock, cmd.encode("utf-8"))
        if cmd == "exit":
            break
        if cmd.startswith("send "):
            _, path = cmd.split(" ",1)
            send_file(sock, path)
        elif cmd.startswith("get "):
            receive_file(sock)
    sock.close()

if __name__ == "__main__":
    main()
