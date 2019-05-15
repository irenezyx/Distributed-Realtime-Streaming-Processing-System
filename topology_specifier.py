def topology_specifier():
    code = '''
def filter(inputlist, stop_flag, socket_list):
    # each input in inputlist is a list of [input,app_no,original_tuple]
    # socket_list[0] = next_ip socket_list[1] = client_ip
    print("##############filter")
    while True:
        if len(stop_flag) != 0:
            print("filter shutdown")
            return
        if len(inputlist)  != 0:
            # this is the body of the function, the rest of the code is boiler plate
            input = inputlist.popleft()
            result = input[0][0]
            if len(result) < 4:
                print("filtering out :" + result)
                continue
            

            for func_addr in socket_list:
                try:
                    # print(func_sock[1].getsockname())
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    
                    sock.connect((func_addr[1]))
                    message = json.dumps(
                        {"input_for_receiver": result, "function_to_run": func_addr[0], "app_no": input[1],
                         "original": input[2]}).encode()
                    # print("sending out this:" + message.decode())
                    # print("here")
                    sock.send(message)
                except Exception as e:
                    print(e)
                    print("send to from filter next ip failed")


def transform(inputlist, stop_flag, socket_list):
    print("##############transform")
    wordcount = {}
    from sdfs_interface import append
    while True:
        if len(stop_flag) != 0:
            print("transform shutdown")
            return
        if len(inputlist) != 0:
            # this is the body of the function, the rest of the code is boiler plate
            input = inputlist.popleft()
            result = input[0]
            if result in wordcount:
                wordcount[result] += 1
            else:
                wordcount[result] = 1
            print(result + ": " + str(wordcount[result]))
            append("frequency",result + ": " + str(wordcount[result])+"\\n")
            # append("frequency",result + ": " + str(wordcount[result])+"\\n")
            # print("transforming: " + result)

            # for sock in socket_list:
            #    try:
            #        sock[1].send(json.dumps({"input_for_receiver":result,"function_to_run":inputlist[1],"app_no"
            #        :inputlist[2],"original":inputlist[3]}).encode())
            #    except Exception as e:
            #        print(e)
            #        print("send to next ip from transform failed") 
    '''

    topology = {"op":"topology","downstreams":{"spout":["filter"],"filter":["transform"],"transform":[]},"filename":"wordcount.txt","code":code,"file":"wordfrequency"}
    return topology
if __name__ == "__main__":
    p = topology_specifier()
    c = '''print("\\n"+"a")'''
    exec(c)