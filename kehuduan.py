import socket
import os
import struct

SERVER_HOST = "103.212.187.100"
SERVER_PORT = 8888
CHUNK_SIZE = 4096

def read_msg(sock):
    sock.settimeout(15.0)
    header = sock.recv(4)
    if len(header) != 4:
        raise ConnectionError("连接断开")
    msg_len = struct.unpack(">I", header)[0]
    buf = b""
    while len(buf) < msg_len:
        part = sock.recv(msg_len - len(buf))
        if not part:
            raise ConnectionError("读取中断")
        buf += part
    return buf

def send_msg(sock, data: bytes):
    sock.sendall(struct.pack(">I", len(data)) + data)

def send_file(sock, filepath):
    if not os.path.isfile(filepath):
        print("[客户端] 文件不存在")
        return
    fname = os.path.basename(filepath)
    fsize = os.path.getsize(filepath)
    send_msg(sock, f"{fname}|{fsize}".encode())
    resp = read_msg(sock)
    if resp != b"READY":
        print("[客户端] 服务端未就绪")
        return
    print(f"[客户端] 上传 {fname}")
    with open(filepath, "rb") as fp:
        while chunk := fp.read(CHUNK_SIZE):
            sock.sendall(chunk)
    print("[客户端] ✅ 上传完成")

def receive_file(sock):
    info = read_msg(sock).decode("utf-8")
    parts = info.split("|", 1)
    if len(parts) != 2:
        print(f"[客户端] 非法消息 {info}")
        return
    fname, size_str = parts
    if fname == "ERROR":
        print(f"[客户端] 错误：{size_str}")
        return
    fsize = int(size_str)
    print(f"[客户端] 接收 {fname} 大小 {fsize}")
    send_msg(sock, b"READY")
    save_dir = "client_recv"
    os.makedirs(save_dir, exist_ok=True)
    path = os.path.join(save_dir, fname)
    received = 0
    with open(path, "wb") as fp:
        while received < fsize:
            remain = fsize - received
            chunk = sock.recv(min(CHUNK_SIZE, remain))
            if not chunk:
                raise ConnectionError("接收中断")
            fp.write(chunk)
            received += len(chunk)
            print(f"[客户端] 进度 {received}/{fsize}")
    print(f"[客户端] ✅ 保存 {path}")

def main():
    sock = socket.socket()
    sock.settimeout(20.0)
    try:
        sock.connect((SERVER_HOST, SERVER_PORT))
        print("✅ 连接成功")
    except Exception as e:
        print(f"❌ 连接失败 {e}")
        return
    print("命令：send 本地路径 | get 服务端路径 | ls | exit")
    while True:
        cmd = input(">>> ")
        send_msg(sock, cmd.encode("utf-8"))
        if cmd == "exit":
            break
        if cmd.startswith("send "):
            _, p = cmd.split(" ", 1)
            send_file(sock, p)
        elif cmd.startswith("get "):
            receive_file(sock)
        elif cmd == "ls":
            lst = read_msg(sock).decode("utf-8")
            print("\n---server_recv 文件列表---")
            print(lst)
            print()
    sock.close()

if __name__ == "__main__":
    main()
