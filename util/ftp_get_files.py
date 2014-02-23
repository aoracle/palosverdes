import sys
import ftplib
import os
from ftplib import FTP
ftp = ftplib.FTP("ftp.eoddata.com")
ftp.login("amahajan1981", "gurukul1")


source="/"
dest="/stocks/raw_data/"

def check_word_type(filename):
    words = ['AMEX','OPRA','NYSE','NASDAQ'] #I am not sure if adj and adv are variables
    count = 0
    for i in words:
        if i in filename:
            word_type = str(i) #just make sure its string
            count=+1
        else:
            word_type = ''
    if count == 1:
        return count

def downloadFiles(path,destination):
#path & destination are str of the form "/dir/folder/something/"
#path should be the abs path to the root FOLDER of the file tree to download
    try:
        ftp.cwd(path)
        #clone path to destination
        os.chdir(destination)
        os.mkdir(destination[0:len(destination)-1]+path)
        print destination[0:len(destination)-1]+path+" built"
    except OSError:
        #folder already exists at destination
        pass
    except ftplib.error_perm:
        #invalid entry (ensure input form: "/dir/folder/something/")
        print "error: could not change to "+path
        sys.exit("ending session")

    #list children:
    filelist=ftp.nlst()

    for file in filelist:
        try:
            if check_word_type(file):
            #this will check if file is folder:
                ftp.cwd(path+file+"/")
            #if so, explore it:
                downloadFiles(path+file+"/",destination)
        except ftplib.error_perm:
            #not a folder with accessible content
            #download & return
            os.chdir(destination[0:len(destination)-1]+path)
            #possibly need a permission exception catch:
            ftp.retrbinary("RETR "+file, open(os.path.join(destination,file),"wb").write)
            print file + " downloaded"
    return

downloadFiles(source,dest)