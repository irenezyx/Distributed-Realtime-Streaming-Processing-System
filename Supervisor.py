import socket
import threading
import collections
import json
import collections
import logging
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
        logging.basicConfig(filename='vm.log', level=logging.INFO)

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
            threading.Thread(target=self.job_handler,args=(message,)).start()
    def application_shutdown(self,app_no):
        #self.input_to_output.pop(app_no)
        for task in self.tasks_parms[app_no].keys():
            self.tasks_parms[app_no][task][1].append(True)
        self.tasks_parms.pop(app_no)
        print("shutting down application no."+str(app_no))


    def job_handler(self,message):
        dict = json.loads(message)
        if "failed" in dict:
            self.application_shutdown(dict["failed"][0])
            return

        #map function to application name
        self.code = dict["code"]
        input_func = dict["inputf"]
        application_num = dict["app_no"]
        if application_num not in self.tasks_parms:
            #self.input_to_output[application_num] = {}
            self.tasks_parms[application_num] = {}
            self.tasks_parms[application_num][input_func] = []
        else:
            self.tasks_parms[application_num][input_func] = []


        #this is not the last bolt. If this is, this should reply to client
        output_to_these_functions_and_ips = [pair for pair in dict['ips']]
        logging.info(str(dict))


        #self.input_to_output[application_num][input_func] = output_to_these_functions_and_ips

        exec(self.code)

        inputlist = collections.deque([])#input list to tuple runner
        stop_flag_list = []
        output_to_these_socket_and_function = []
        if "client" not in dict:
            for pair in output_to_these_functions_and_ips:
                next_sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                #print("i m binding to this motherfucker:" + pair[1][0])
                #next_sock.connect((pair[1][0],8011))
                #print("finish bounding to this motherfucker:" + pair[1][0])
                output_to_these_socket_and_function.append((pair[0],(pair[1][0],8011)))#take the first ip of the downstream

        #client_sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        #client_sock.connect((dict["client"],3333))

        #output_to_these_socket_and_function.append(client_sock)

        #initialization for a task, things each new tuple needs when coming
        self.tasks_parms[application_num][input_func].append(inputlist)

        self.tasks_parms[application_num][input_func].append(stop_flag_list)
        self.tasks_parms[application_num][input_func].append(output_to_these_socket_and_function)

        exec("threading.Thread(target="+input_func+",args=(inputlist,stop_flag_list,output_to_these_socket_and_function"
                                                   ")).start()")


    #def job_runner(self,func_name):

    def tuple_listen(self):

        while True:
            #print("tuple_listen start")
            conn, addr = self.tuple_listen_socket.accept()
            #print("tuple_listen start")

            message = ""
            while True:
                data = conn.recv(8192)
                message += str(data.decode())
                #print(message)
                if len(data) == 0:
                    break
                elif len(message) != 8192 and message.endswith("}"):
                    break
            threading.Thread(target=self.tuple_handler,args=(message,)).start()

    def tuple_handler(self,message):
        #print("loading")
        #print(message)
        dict = json.loads(message)
        #print("received dict in tuple handler is:\n"+str(dict))
        input_function_name = dict["function_to_run"]
        app_no = dict["app_no"]
        original_tuple = dict["original"]
        input = dict["input_for_receiver"]
        input = [input,app_no,original_tuple]
        #append to the inputlist for the function of this application
        self.tasks_parms[app_no][input_function_name][0].append(input)







if __name__ == "__main__":
    from peer4 import Peer
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
