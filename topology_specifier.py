def topology_specifier():
    code = '''
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

            socks = socket_list[0]
            for func_sock in socks[:]:
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
            

            socks = socket_list[0]
            for sock in socks:
                try:
                    sock.send({"input_for_receiver":result,"function_to_run":inputlist[1],"app_no"
                    :inputlist[2],"original":inputlist[3]})
                except Exception as e:
                    print(e)
                    print("send to next ip failed")       
    '''

    topology = {"op":"topology","downstreams":{"spout":["filter"],"filter":["transform"],"transform":[]},"filename":"wordcount.txt","code":code,"file":"wordfrequency"}
    return topology
