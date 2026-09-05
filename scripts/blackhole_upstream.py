
import socket, threading, time, sys
port = int(sys.argv[1]); hold = float(sys.argv[2])
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind(("127.0.0.1", port)); s.listen(64)
def h(c):
    try:
        c.settimeout(hold); 
        try: c.recv(65536)
        except Exception: pass
        time.sleep(hold)
    finally:
        try: c.close()
        except Exception: pass
while True:
    c,_ = s.accept(); threading.Thread(target=h, args=(c,), daemon=True).start()
