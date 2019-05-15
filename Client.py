import sys
import socket
import json
import threading
import socket
def start(filename,sdfsfilename):
    w = open(filename,'r')

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
    message_handler(message,w)

def message_handler(message,w):
    dict = json.loads(message)

    app_no = dict["app_no"]
    soc_lists = []
    for ip in dict["ips"][0][1]:
        soc_lists.append(ip)
    import time
    time.sleep(5)
    for line in w:
        dict = json.dumps({"input_for_receiver":line,"original":line,"function_to_run":"filter","app_no":app_no}).encode()
        ip = soc_lists[(ord(line[0]) - 97) % len(soc_lists)]
        print(ip)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(("fa18-cs425-g26-05.cs.illinois.edu", 8011))
        message = "{hello}"

        sock.send(message.encode())




def stops(app_no):
    master_ip = "fa18-cs425-g26-01.cs.illinois.edu"
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((master_ip,8000))
    dict = {"stop":app_no}
    sock.send(json.dumps(dict).encode())
    data = sock.recv(8092).decode()
    print(data)

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
def reply_listen():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("0.0.0.0",8011))
    sock.listen(5)
    while True:
        conn, addr = sock.accept()
        data = conn.recv(8192).decode()
        message = json.dumps(data)
        print(message)
        conn.close()
'''
if __name__ == "__main__":
    #threading.Thread(target=reply_listen).start()
    import time
    time.sleep(1)
    start("wordcount.txt", "frequency")


    '''
    if sys.argv[0] == "start":
        start(sys.argv[1],sys.argv[2])
    elif sys.argv[0] == "stop":
        stops(sys.argv[1])
    '''
