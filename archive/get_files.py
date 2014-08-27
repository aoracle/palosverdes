import ftplib
import threading

def download(ftp,directory,file):
    ftp.cwd(directory)
    sock = ftp.transfercmd('RETR ' + file)
    def background():
        f = open(file,"wb")
        while True:
            block = sock.recv(1024*1024)
            if not block:
                break
            f.write(block)
        sock.close()
    t = threading.Thread(target=background)
    t.start()
    while t.is_alive():
        t.join(60)
        ftp.voidcmd('NOOP')
    #f = open(file,"wb")
    #ftp.retrbinary("RETR " + file,f.write)
    #f.close()
    
ftp = ftplib.FTP("ftp.eoddata.com")
ftp.login("amahajan1981", "gurukul1")

download(ftp, "/", "OPRA_20140811.txt")
download(ftp, "/", "OPRA_20140812.txt")
download(ftp, "/", "OPRA_20140813.txt")
download(ftp, "/", "OPRA_20140814.txt")
download(ftp, "/", "OPRA_20140815.txt")

