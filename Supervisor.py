import socket
import threading
import collections
import json
import collections

class Supervisor:
    def __init__(self):
        self.job_listen_sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.job_listen_sock.bind(("0.0.0.0", 8010))
        self.job_listen_sock.listen(5)
        self.tuple_listen_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.tuple_listen_socket.bind(("0.0.0.0", 8011))
        self.tuple_listen_socket.listen(5)
        self.code = ""
        self.input_to_output = {"5":{"input_function":"output_function"}}
        self.tasks_parms = {"5":{"inputf":[["input"],[],[],[]],"inputf1":[[],[],[],[]]}}
        threading.Thread(target=self.job_listen).start()
        threading.Thread(target=self.tuple_listen).start()

    def job_listen(self):
        while True:
            conn,addr = self.job_listen_sock.accept()
            message = ""
            while True:
                data = conn.recv(8192)
                message += str(data.decode())
                if len(data) == 0:
                    break
                elif len(message) != 8192 and message.endswith("}"):
                    break
            self.job_handler(message)
    def application_shutdown(self,app_no):
        self.input_to_output.pop(app_no)
        for task in self.tasks_parms[app_no].keys():
            self.tasks_parms[app_no][task][1].append(True)
        self.tasks_parms.pop(app_no)


    def job_handler(self,message):
        dict = json.loads(message)
        if "stop" in dict:
            self.application_shutdown(dict["app_no"])
            return

        #map function to application name
        self.code = dict["code"]
        input_func = dict["inputf"]
        application_num = dict["app_no"]
        if application_num not in self.input_to_output:
            self.input_to_output[application_num] = {}
            self.tasks_parms[application_num] = {}
            self.tasks_parms[application_num][input_func] = []


        #this is not the last bolt. If this is, this should reply to client
        output_to_these_functions_and_ips = [pair for pair in dict['ips']]
        print(dict)


        self.input_to_output[application_num][input_func] = output_to_these_functions_and_ips

        exec(self.code)

        inputlist = collections.deque([])#input list to tuple runner
        stop_flag_list = []
        output_to_these_socket_and_function = []
        for pair in output_to_these_functions_and_ips:
            next_sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            next_sock.connect((pair[1][0],8011))
            output_to_these_socket_and_function.append((pair[0],next_sock))

        #client_sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        #client_sock.connect((dict["client"],3333))

        #output_to_these_socket_and_function.append(client_sock)

        #initialization for a task, things each new tuple needs when coming
        self.tasks_parms[application_num][input_func].append(inputlist)

        self.tasks_parms[application_num][input_func].append(stop_flag_list)
        self.tasks_parms[application_num][input_func].append(output_to_these_socket_and_function)

        exec("threading.Thread(target="+input_func+",args=(inputlist,stop_flag_list,output_to_these_socket_and_function."
                                                   ")).start()")


    #def job_runner(self,func_name):

    def tuple_listen(self):
        while True:
            conn, addr = self.job_listen_sock.accept()
            message = ""
            while True:
                data = conn.recv(8192)
                message += str(data.decode())
                if len(data) == 0:
                    break
                elif len(message) != 8192 and message.endswith("}"):
                    break
            self.tuple_handler(message)

    def tuple_handler(self,message):
        dict = json.loads(message)
        input_function_name = dict["function_to_run"]
        app_no = dict["app_no"]
        original_tuple = dict["original"]
        input = dict["input_for_receiver"]
        input = [input,app_no,original_tuple]
        self.tasks_parms[app_no][input_function_name].append(input)



def filter(inputlist,stop_flag,socket_list):
#each input in inputlist is a list of [input,app_no,original_tuple]
#socket_list[0] = next_ip socket_list[1] = client_ip
    while True:
       if len(stop_flag)!=0:
           return
       if len(inputlist)!=0:
           #this is the body of the function, the rest of the code is boiler plate
            input = inputlist.popleft()
            result = input[0]
            if len(result) < 10:
                continue
                '''
                try:
                    socket_list[1].send({"original":input[3]})
                except Exception as e:
                    print(e)
                    print("client isn't receiving "+input[3])
                '''

            socks = socket_list[0]
            for func_sock in socks[:-1]:
                try:
                    func_sock[1].send({"input_for_receiver":result,"function_to_run":func_sock[0],"app_no"
                    :input[1],"original":input[2]})
                except Exception as e:
                    print(e)
                    print("send to next ip failed")
def tranform(inputlist,stop_flag,socket_list):
    wordcount = {}
    while True:
       if len(stop_flag)!=0:
           return
       if len(inputlist)!=0:
           #this is the body of the function, the rest of the code is boiler plate
            input = inputlist.popleft()
            result = input[0]
            print(result)
            '''
            try:
                socket_list[1].send({"original":input[3]})
            except Exception as e:
                print(e)
                print("client isn't receiving "+input[3])
            '''

            socks = socket_list[0]
            for sock in socks:
                try:
                    sock.send({"input_for_receiver":result,"function_to_run":inputlist[1],"app_no"
                    :inputlist[2],"original":inputlist[3]})
                except Exception as e:
                    print(e)
                    print("send to next ip failed")






if __name__ == "__main__":
    from peer import Peer
    supervisor = Supervisor()
    host_name = socket.gethostname()
    p = Peer(host_name)
    p.start()

    '''
    li = collections.deque([])
    if li:
        print("e")

    exec(code)

    stop_flag = []
    exec("threading.Thread(target=bar,args=[li,stop_flag]).start()")
    for i in range(0,200000):
        li.append("a")
    stop_flag.append("stop")
    '''
