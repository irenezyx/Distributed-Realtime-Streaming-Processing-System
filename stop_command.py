import socket
if __name__ == "__main__":

    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect(("0.0.0.0", 8010))
    app_no = 2
    import sys
    if len(sys.argv) > 1:
        app_no = sys.argv[1]
    message = {"op": "stop", "stop": app_no}
    import json
    sock.send(json.dumps(message).encode())

