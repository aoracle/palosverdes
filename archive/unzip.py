#! /usr/bin/python 

import zipfile,os

def zip():
   fh = open('/home/ec2-user/Stocks/AMEX_2013.zip', 'rb')
   z = zipfile.ZipFile(fh)
   for name in z.namelist():
      #outpath = "/home/ec2-user/Stocks/load"
      outpath = "/home/ec2-user/Stocks/load_%s" %("AMEX")
      if os.path.exists(outpath)==False:
         os.mkdir(outpath)
         os.chmod(outpath, 0755)
      z.extract(name, outpath)
   fh.close()


if __name__ == 	'__main__':
   zip()







