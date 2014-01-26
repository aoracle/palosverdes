import ftplib

def download(ftp,directory,file):
    ftp.cwd(directory)
    f = open(file,"wb")
    ftp.retrbinary("RETR " + file,f.write)
    f.close()
    
ftp = ftplib.FTP("ftp.eoddata.com")
ftp.login("amahajan1981", "gurukul1")

download(ftp, "/", "OPRA_20140114.txt")