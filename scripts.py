#! /usr/bin/python 

import zipfile,os

def zip():
   for i in os.listdir('/home/ec2-user/Stocks/'):
    if i.endswith(".zip"): 
       fh = open('/home/ec2-user/Stocks/%s'%(i) , 'rb')
       z = zipfile.ZipFile(fh)
       for name in z.namelist():
          #outpath = "/home/ec2-user/Stocks/load"
          exchange = i.split('_', 1)[0].replace('.', '').upper()
          outpath = "/home/ec2-user/Stocks/load_%s" %(exchange)
          if os.path.exists(outpath)==False:
             os.mkdir(outpath)
             os.chmod(outpath, 0755)
          z.extract(name, outpath)
       fh.close()
        #print i
        #continue
    #else:
    #    continue	



if __name__ == 	'__main__':
   zip()







