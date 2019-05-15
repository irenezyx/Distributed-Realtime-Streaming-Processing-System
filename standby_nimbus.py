import socket
import json
import threading
import random
from introducer4 import introducer

class Nimbus:
    def __init__(self, membership_list):
        self.membership_list = membership_list
        
        self.op_listen_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.op_listen_socket.bind(("0.0.0.0", 8000))
        self.op_listen_socket.listen(5)
        
        self.app_no = 0
        
        self.app_topology_dict = {}
        # app_no: {bolt_func_name: [upstream_bolt, downstream_bolts]}
        # where, down: [bolt_func_name, ...], up: addr
        
        self.bolt_addr_dict = {} # app_np: {bolt: [addr1, addr2, ...]}
        self.addr_bolt_dict = {} # app_no: {addr: [bolt1, bolt2, ...]}
        self.file_number = {} # app_no: {addr: [#,...]} 
        self.client_addr = {} # app_no: addr

        threading.Thread(target=self.check_node_fail).start()

        threading.Thread(target=self.listen, args=(self.op_listen_socket,)).start()

        
    def listen(self, sock):
        max_data = 8192
        #print('start listen, sock: {0}, ack: {1}'.format(sock, ack))
        while True:
            message = ""
            connection, client_address = sock.accept()
            while True:
                data = connection.recv(max_data)
                message += str(data.decode())
                if len(data) == 0:
                    break
                elif len(data) != max_data and message.endswith("}"):
                    break
        
            try:
                message = json.loads(message)
            except:
                #print('at line 43 in nimbus.py, can\'t json.loads(message): {0}'.format(message))
                continue
            
            client_address = socket.gethostbyaddr(client_address[0])[0]
            
            threading.Thread(target=self.message_handler, args=(message, client_address, connection)).start()
        
    def message_handler(self, message, client_address, connection):
        if 'op' not in message:
            #print('why do I (nimbus) receive a message without "op"?')
            return
        
        if message['op'] == 'stop':
            try:
                app_no = message['app_no']
            except:
                return
            node_message = {'failed': [app_no]}
            for addr in self.addr_bolt_dict[app_no]:
                try:
                    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                    sock.connect((addr, 8010))
                    sock.send(json.dumps(node_message).encode())
                    sock.close()
                    #print('nimbus sent message <{0}> to <{1}> for op stop'.format(message, addr))
                except Exception as e:
                    #print('failed to send message <{0}> to <{1}> for op stop'.format(message, addr))
                    print(e)
            try:
                addr = self.client_addr[app_no]
                message['op'] = 'finish'
                sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                sock.connect((addr, 8010))
                sock.send(json.dumps(message).encode())
                sock.close()
                #print('nimbus sent message <{0}> to <{1}> for op stop'.format(message, addr))
            except Exception as e:
                #print('failed to send message <{0}> to <{1}> for op stop'.format(message, addr))
                print(e)
            self.app_topology_dict.pop(app_no, None)
            self.bolt_addr_dict.pop(app_no, None)
            self.addr_bolt_dict.pop(app_no, None)
            self.file_number.pop(app_no, None)
            self.client_addr.pop(app_no, None)
            return
        
        # 'op' == 'topology'
        if message['op'] != 'topology':
            #print('why do I, nimbus, receive a message op not "topology" or "stop"?')
            return
        if 'downstreams' not in message:
            #print('nimbus receives message lacking downstreams.')
            return
        if 'code' not in message:
            #print('nimbus receives message lacking code.')
            return
        '''
        self.app_topology_dict = {}
        # app_no: {bolt_func_name: [upstream_bolt, downstream_bolts]}
        # where, down: [bolt_func_name, ...], up: bolt
        
        self.bolt_addr_dict = {} # app_no: {bolt: [addr1, addr2, ...]}
        self.addr_bolt_dict = {} # app_no: {addr: [bolt1, ...]}
        self.file_number = {} # app_no: {addr: [#,...]} 
        self.client_addr = {no: addr}
        '''
        self.app_no += 1
        #print('nimbus processing application {0}'.format(self.app_no))
        self.client_addr[self.app_no] = client_address
        no_topology_dict = {}; no_bolt_addr_dict = {}; no_addr_bolt_dict = {}; no_file_number = {}
        topo = message['downstreams'] # a dict with { bolt: [child1_bolt, ...] }
        bolts = list(topo.keys()) # bolt names same with function names
        #print('bolts: {0}'.format(bolts))
        ips_dict = {}
        flag = False # if has assigned ips to bolt that's not spout
        for bolt in bolts: # bolt is 'spout'...
            downstream_bolts = topo[bolt]
            if bolt == 'spout': # len(old_bolt_name) == 0
                inputf = '_'
                no_bolt_addr_dict[bolt] = [client_address]
                no_addr_bolt_dict[client_address] = [bolt]
                no_file_number[client_address] = [-1]
                no_topology_dict[bolt] = [None, downstream_bolts]
            else:
                inputf = bolt
                if bolt in no_topology_dict:
                    no_topology_dict[bolt][1] = downstream_bolts
                else:
                    no_topology_dict[bolt] = ['_', downstream_bolts]
                # if not spout,
                # then its addr must have been determined by its upstream
            
            #print('downstram bolts: {0}'.format(downstream_bolts))
            ips = []
            for child in downstream_bolts: 
                if child not in no_topology_dict:
                    no_topology_dict[child] = [bolt, []]
                else:
                    no_topology_dict[child][0] = bolt
                # update bolt->addr and addr->bolt for downstream bolts
                if flag:
                    for tmp_bolt in bolts:
                        if tmp_bolt != child and tmp_bolt != 'spout':
                            break
                    downstream_bolt_addrs = no_bolt_addr_dict[tmp_bolt]
                else:
                    chosen_indexes = random.sample(range(1, len(self.membership_list)), min(len(self.membership_list)-1, 3))
                    downstream_bolt_addrs = [self.membership_list[i].split('#')[0] for i in chosen_indexes]
                #print('downstram_bolt_addrs: {0} assigned for child {1}'.format(downstream_bolt_addrs, child))
                for i, child_addr in enumerate(downstream_bolt_addrs):
                    #print('i: {0}, child_addr:{1}'.format(i, child_addr))
                    if child_addr in no_addr_bolt_dict:
                        assert child not in no_addr_bolt_dict[child_addr], 'this bolt {0} already used on addr {1}'.format(child, child_addr)
                        no_addr_bolt_dict[child_addr].append(child)
                        no_file_number[child_addr].append(i)
                    else:
                        no_addr_bolt_dict[child_addr] = [child]   
                        no_file_number[child_addr] = [i]
                    #print('no_addr_bolt_dict: {0}'.format(no_addr_bolt_dict))
                    #print('no_file_number: {0}'.format(no_file_number))
                    if child in no_bolt_addr_dict:
                        no_bolt_addr_dict[child].append(child_addr)
                    else:
                        flag = True
                        no_bolt_addr_dict[child] = [child_addr] 
                    #print('no_bolt_addr_dict: {0}'.format(no_bolt_addr_dict))
                ips.append([child, downstream_bolt_addrs])
            ips_dict[bolt] = ips
         
        #print('no_topology_dict: {0}'.format(no_topology_dict))   
        # print('ips_dict: {0}'.format(ips_dict))
        for bolt in bolts:
            # send message to each bolt's addrs
            #print('ready to send message for the node of bolt {0}'.format(bolt))
            addrs = no_bolt_addr_dict[bolt]
            inputf = bolt if bolt != 'spout' else '_'
            # print('\n addrs: {0}'.format(addrs))
            for addr in addrs:
                #print('\n addr: {0}'.format(addr))
                #file_num = no_file_number[addr][no_addr_bolt_dict[addr].index(bolt)]
                message_by_nimbus = {'op': 'next', 
                                     'code': message['code'],
                                     'inputf': inputf,
                                     'app_no': self.app_no,
                                     'ips': ips_dict[bolt],
                                     'file': message['file']}
                if bolt != 'spout':
                    for i in range(len(ips_dict[bolt])):
                        message_by_nimbus['ips'][i][1] = [addr]
                if len(no_topology_dict[bolt][1]) == 0: # last bolt has no downstream bolts
                    message_by_nimbus['ips'] = [ ['_', [client_address]] ]
                    message_by_nimbus['client'] = client_address
                try:
                    if addr == client_address:
                        connection.send(json.dumps(message_by_nimbus).encode())
                        #print('nimbus sent ips <{0}>, inputf <{2}> to <{1}'.format(message_by_nimbus['ips'], client_address, inputf))
                    else:
                        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                        sock.connect((addr, 8010))
                        sock.send(json.dumps(message_by_nimbus).encode())
                        sock.close()
                        #print('nimbus sent ips <{0}>, inputf <{2}> to <{1}>'.format(message_by_nimbus['ips'], addr, inputf))
                except Exception as e:
                    #print('nimbus failed to send ips <{0}>, inputf <{2}> to <{1}>'.format(message_by_nimbus['ips'], addr, inputf))
                    print(e)

        # update self dicts
        self.app_topology_dict[self.app_no] = no_topology_dict
        self.bolt_addr_dict[self.app_no] = no_bolt_addr_dict
        self.addr_bolt_dict[self.app_no] = no_addr_bolt_dict
        self.file_number[self.app_no] = no_file_number
        self.client_addr[self.app_no] = client_address
        #print('app_no {0} finishes in nimbus'.format(self.app_no))
        
    def copy_membership_list(self):
        mem_copy = [mem.split('#')[0] for mem in self.membership_list]
        return mem_copy
    
    def check_membership_list_len(self):
        return len(self.membership_list)
    
    def check_node_fail(self):
        list_old = self.copy_membership_list()
        failed_mems = []
        while True:
            len_old = len(list_old)
            len_new = self.check_membership_list_len()
            if len_new == len_old:
                continue
            list_new = self.copy_membership_list()
            #print('len changed from {2} to {3};   membership list from {0} \n to {1}'.format(list_old, list_new, len_old, len_new))
            for mem in list_old:
                if mem not in list_new:
                    failed_mems.append(mem)
            list_old = list_new
            if 'fa18-cs425-g26-02.cs.illinois.edu' not in failed_mems:
                continue
            print('detected nimbus failure, standby master comes out now')
            failed_mems = failed_mems[-2:]
            #print('failed: {0}'.format(failed_mems))
            self.recover_failed_nodes(failed_mems)
            failed_mems = []
    
    def recover_failed_nodes(self, failed_mems):
        #print('failed mems:{0}'.format(failed_mems))
        infected_app_nos = []
        addr_infected_apps = {} # ip: [app_no1, app_no2, ...]
        for failed_mem in failed_mems:
            addr_infected_apps.pop(failed_mem, None)
            #print('app nos dict to be processed: {0}'.format(self.addr_bolt_dict))
            for app_no in self.addr_bolt_dict:
                if app_no in infected_app_nos:
                    print('app_no {0} already processed'.format(app_no))
                    continue
                infected_app_nos.append(app_no)
                if failed_mem in self.addr_bolt_dict[app_no]: # this app_no influenced
                    for ip in self.addr_bolt_dict[app_no]:
                        #print('adding ip: {0}'.format(ip))
                        if ip != failed_mem:
                            if ip not in addr_infected_apps:
                                addr_infected_apps[ip] = [app_no]
                            else:
                                addr_infected_apps[ip].append(app_no)
                        
        #print('infected_app_nos_dict: {0}'.format(addr_infected_apps))
        #print('infected app nos: {0}'.format(infected_app_nos))
        addrs = list(addr_infected_apps.keys()) 
        # inform client and all infected nodes about the failed applications
        for addr in addrs:
            if addr not in self.membership_list:
                continue
            message = {'failed': addr_infected_apps[addr]} # values is [#, #, ...]
            try:
                sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                sock.connect((addr, 8010))
                sock.send(json.dumps(message).encode())
                sock.close()
                #print('nimbus sent message <{0}> to <{1}>'.format(message, addr))
            except Exception as e:
                #print('failed to send message <{0}> to <{1}>'.format(message, addr))
                print(e)
        
        # update dicts
        for app_no in infected_app_nos:
            self.app_topology_dict.pop(app_no, None)
            self.bolt_addr_dict.pop(app_no, None)
            self.addr_bolt_dict.pop(app_no, None)
            self.file_number.pop(app_no, None)
            self.client_addr.pop(app_no, None)
            
if __name__ == '__main__':
    intro = introducer()
    threading.Thread(target=intro.message_list_maintainer).start()
    threading.Thread(target=intro.listen).start()
    nimbus = Nimbus(intro.membership_list)
