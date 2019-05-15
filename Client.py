import sys
import socket
import json
import threading
from sdfs_interface import Client
import socket
def start(filename,sdfsfilename,stop_var):
    w = open(filename,'r')

    client = Client()
    client.put("noop", sdfsfilename)
    client.sock.close()
    client.listen_socket.close()
    import topology_specifier
    topo = topology_specifier.topology_specifier()
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect(("fa18-cs425-g26-01.cs.illinois.edu",8000))
    st = json.dumps(topo)
    sock.send(st.encode())
    message = ""
    while True:
        data = sock.recv(8192).decode()
        message += data
        if len(data) == 0 or (len(data) < 8192 and data.endswith("}")):
            break
    message_handler(message,w,stop_var)

def message_handler(message,w,stop_var):
    dict = json.loads(message)

    app_no = dict["app_no"]
    soc_lists = []
    for ip in dict["ips"][0][1]:
        soc_lists.append(ip)
    import time
    time.sleep(3)
    for line in w:
        if len(stop_var) != 0:
            raise ValueError('A very specific bad thing happened')
        time.sleep(1)
        line = line.strip('n')
        tuple = eval(line)
        dict = json.dumps({"input_for_receiver":tuple,"original":tuple,"function_to_run":"filter","app_no":app_no}).encode()
        ip = soc_lists[(ord(tuple[0][0]) - 97) % len(soc_lists)]

        print(ip)
        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        sock.connect((ip, 8011))
        sock.send(dict)
        sock.close()
    import os

    os._exit(0)



def stops(app_no):
    master_ip = "fa18-cs425-g26-01.cs.illinois.edu"
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((master_ip, 8000))
    dict = {"stop": app_no}
    sock.send(json.dumps(dict).encode())
    data = sock.recv(8092).decode()
    print(data)
'''
def listen(sock):
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    while True:
        conn,addr = sock.accept()
        data= conn.recv(8192).decode()
        message = json.dumps(data)
        if "stop" in message:
            stops(message['app_no'])
        start("wordcount.txt", "frequency")
'''

def reply_listen(var):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("0.0.0.0",8010))
    sock.listen(5)
    while True:
        conn, addr = sock.accept()
        data = conn.recv(8192).decode()
        message = json.loads(data)

        if "failed" in message:
            var[0].append(0)
        elif "stop" in message:
            var[0].append(0)
            import os
            os._exit(0)

if __name__ == "__main__":
    li = [[]]
    threading.Thread(target=reply_listen,args=(li,)).start()
    import time
    time.sleep(1)
    import sys
    input_file = "wordcount.txt"
    output_file = "frequency"
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        output_file = sys.argv[2]

    while True:
        stop_var = []
        li[0] = stop_var
        try:
            start(input_file, output_file,stop_var)
            break
        except Exception as e:
            print(e)
            print("failure recovery")
            time.sleep(10)






