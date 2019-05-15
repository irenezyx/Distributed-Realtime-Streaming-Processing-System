import socket
import json
import threading
import random
from introducer import introducer

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
                print('at line 43 in nimbus.py, can\'t json.loads(message): {0}'.format(message))
                continue
            
            client_address = socket.gethostbyaddr(client_address[0])[0]
            
            self.message_handler(message, client_address, connection)
        
    def message_handler(self, message, client_address, connection):
        if 'op' not in message:
            print('why do I (nimbus) receive a message without "op"?')
            return
        
        if 'op' == 'stop':
            app_no = message['app_no']
            addr_list = list(self.addr_bolt_dict[app_no].keys())
            for addr in addr_list:
                try:
                    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                    sock.connect((addr, 8010))
                    sock.send(json.dumps(message).encode())
                    sock.close()
                    print('nimbus sent message <{0}> to <{1}> for op stop'.format(message, addr))
                except Exception as e:
                    print('failed to send message <{0}> to <{1}> for op stop'.format(message, addr))
                    print(e)
            try:
                addr = self.client_addr[app_no]
                message['op'] = 'finish'
                sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                sock.connect((addr, 8010))
                sock.send(json.dumps(message).encode())
                sock.close()
                print('nimbus sent message <{0}> to <{1}> for op stop'.format(message, addr))
            except Exception as e:
                print('failed to send message <{0}> to <{1}> for op stop'.format(message, addr))
                print(e)
            return
        
        # 'op' == 'topology'
        if message['op'] != 'topology':
            print('why do I, nimbus, receive a message op not "topology" or "stop"?')
            return
        if 'downstreams' not in message:
            print('nimbus receives message lacking downstreams.')
            return
        if 'code' not in message:
            print('nimbus receives message lacking code.')
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
        self.client_addr[self.app_no] = client_address
        no_topology_dict = {}; no_bolt_addr_dict = {}; no_addr_bolt_dict = {}; no_file_number = {}
        topo = message['downstreams'] # a dict with { bolt: [child1_bolt, ...] }
        bolts = list(topo.keys()) # bolt names same with function names
        print(bolts)
        ips_dict = {}
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
            
            print('downstram bolts: {0}'.format(downstream_bolts))
            ips = []
            for child in downstream_bolts: 
                if child not in no_topology_dict:
                    no_topology_dict[child] = [bolt, []]
                else:
                    no_topology_dict[child][0] = bolt
                # update bolt->addr and addr->bolt for downstream bolts
                downstream_bolt_addrs = random.sample(self.membership_list, min(len(self.membership_list), 5))
                downstream_bolt_addrs = [downstream_bolt_addrs[i].split('#')[0] for i in range(len(downstream_bolt_addrs))]
                print('downstram_bolt_addrs: {0}'.format(downstream_bolt_addrs))
                for i, child_addr in enumerate(downstream_bolt_addrs):
                    print('i: {0}, child_addr:{1}'.format(i, child_addr))
                    if child_addr in no_addr_bolt_dict:
                        assert child not in no_addr_bolt_dict[child_addr], 'this bolt {0} already used on addr {1}'.format(child, child_addr)
                        no_addr_bolt_dict[child_addr].append(child)
                        no_file_number[child_addr].append(i)
                    else:
                        no_addr_bolt_dict[child_addr] = [child]   
                        no_file_number[child_addr] = [i]
                    print('no_addr_bolt_dict: {0}'.format(no_addr_bolt_dict))
                    print('no_file_number: {0}'.format(no_file_number))
                    if child in no_bolt_addr_dict:
                        no_bolt_addr_dict[child].append(child_addr)
                    else:
                        no_bolt_addr_dict[child] = [child_addr] 
                    print('no_bolt_addr_dict: {0}'.format(no_bolt_addr_dict))
                ips.append([child, downstream_bolt_addrs])
            ips_dict[bolt] = ips
         
        print('no_topology_dict: {0}'.format(no_topology_dict))   
        print('ips_dict: {0}'.format(ips_dict))
        count = 0; length = len(bolts)
        for bolt in bolts:
            # send message to each bolt's addrs
            count += 1
            addrs = no_bolt_addr_dict[bolt]
            print('addrs: {0}'.format(addrs))
            for addr in addrs:
                print('addr: {0}'.format(addr))
                file_num = no_file_number[addr][no_addr_bolt_dict[addr].index(bolt)]
                message_by_nimbus = {'op': 'next', 
                                     'code': message['code'],
                                     'inputf': inputf,
                                     'app_no': self.app_no,
                                     'ips': ips_dict[bolt],
                                     'file': message['file'] + str(file_num),
                                     'client': client_address}
                if count == length:
                    message_by_nimbus['last'] = client_address
                    message_by_nimbus['ips'] = client_address
                try:
                    if addr == client_address:
                        connection.send(json.dumps(message_by_nimbus).encode())
                    else:
                        sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                        sock.connect((addr, 8010))
                        sock.send(json.dumps(message_by_nimbus).encode())
                        sock.close()
                        #print('nimbus sent message <{0}> to <{1}>'.format(message_by_nimbus, addr))
                except Exception as e:
                    #print('failed to send message <{0}> to <{1}>'.format(message_by_nimbus, addr))
                    print(e)

        # update self dicts
        self.app_topology_dict[self.app_no] = no_topology_dict
        self.bolt_addr_dict[self.app_no] = no_bolt_addr_dict
        self.addr_bolt_dict[self.app_no] = no_addr_bolt_dict
        self.file_number[self.app_no] = no_file_number
        print('app_no {0} finishes in nimbus'.format(self.app_no))
        
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
            if len(failed_mems) > 1:
                self.recover_failed_nodes(failed_mems)
                failed_mems = []
    '''
    self.app_topology_dict = {}
    # app_no: {bolt_func_name: [upstream_bolt, downstream_bolts]}
    # where, down: [bolt_func_name, ...], up: bolt
    
    self.bolt_addr_dict = {} # app_no: {bolt: [addr1, addr2, ...]}
    self.addr_bolt_dict = {} # app_no: {addr: [bolt1, ...]}
    self.file_number = {} # app_no: {addr: [#,...]} 
    self.client_addr = {no: addr}
    '''            
    def recover_failed_nodes(self, failed_mems):
        infected_app_nos = []
        flag = True
        for failed_mem in failed_mems:
            for app_no in self.addr_bolt_dict:
                if failed_mem in self.addr_bolt_dict[app_no]:
                    if flag or app_no not in infected_app_nos:
                        infected_app_nos.append(app_no)
        
        message = { 'failed': infected_app_nos }
        for app_no in infected_app_nos:
            addrs = list(self.file_number[app_no].keys())
            addrs.append(self.client_addr[app_no])
            for addr in addrs:
                try:
                    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                    sock.connect((addr, 8010))
                    sock.send(json.dumps(message).encode())
                    sock.close()
                    print('nimbus sent message <{0}> to <{1}>'.format(message, addr))
                except Exception as e:
                    print('failed to send message <{0}> to <{1}>'.format(message, addr))
                    print(e)

if __name__ == '__main__':
    intro = introducer()
    threading.Thread(target=intro.message_list_maintainer).start()
    threading.Thread(target=intro.listen).start()
    nimbus = Nimbus(intro.membership_list)
