#####################################################################################################
# FILENAME : GetYahoo.py
# PURPOSE  : Gets stocks data from finance.yahoo.com
# USAGE    : use one of the following ways to call:
#---------------------------------------------------------------------
#                (1) to get data for one symbol for the given day:
'''
python GetYahoo.py --symbol='AAPL', --date='2012-07'
'''
#---------------------------------------------------------------------
#                (2) to get data for one symbol with default today:
'''
python GetYahoo.py --symbol='AAPL'
'''
#---------------------------------------------------------------------
#                (3) to get data for all symbols (options.txt file required in the module's directory):
# NOTE     : script ignores --symbol and --date options when --exact option is set
'''
python GetYahoo.py --exact='options.txt'
'''
# NOTE     : file should have the following format, otherwise script decided to stop
#            <SYMBOL> <DATE>
#            <DATE> can be omited to use current date
'''
TSL 1401
TSL
GOOG 1207
GOOG
GOOG 1209
'''
#---------------------------------------------------------------------
#                (4) to get data for all symbols from list defined in file:
'''
python GetYahoo.py --alldates='list.txt'
'''
# NOTE     : when using --alldates,  option file should contain only symbols, dates will be ignored.
#            file given by --exact option is ignored too
#input file format:
'''
A
AA
AACC
AAI
...
'''
#####################################################################################################


#####################################################################################################
#                                   MODIFICATION HISTORY
#####################################################################################################
# Version    Date          Author           Reason
#####################################################################################################
# 0.1        20-Jun-2012   Brilenkov A.     Initial revision. Open url, read data and put it to csv.
# 0.2        25-Jun-2012   Brilenkov A.     Fixed bug with <b> tag - zeros printed to csv instead of 
#                                           real numbers.
# 1.0        29-Jun-2012   Brilenkov A.     Script was completely redesigned. Added feature to get data
#                                           for all avalable links starting from today's date.
#                                           Added descriptions, headers, examples etc.
#                                           For skipped symbols additional 'skipped.log' is created.
#####################################################################################################

#####################################################################################################
#                                  INCLUDE REQUIRED MODULES
#####################################################################################################
from bs4 import BeautifulSoup  # to easy parse html
from datetime import date                # we will need to use today's date as default
import urllib, urllib2                   # standart libraries for http requests
import os                                # we shall use options file for reading and
                                         # csv file for appeding results
import re                                # using regex
import optparse                          # script has 'parameters' mode without options file
import sys                               # we need some info from system

#####################################################################################################
#                                            FUNCTIONS
#####################################################################################################


##################################
# Name       : module_path
# Purpose    : Determine current path
# Usage      : module_path()
# Parameters : none
# Returns    : current path with '\'
# Note       : if run on windows - use 
#  !!!         return os.path.dirname(unicode(__file__, sys.getfilesystemencoding( ))) + '\\'
#  !!!         otherwise uncomment and comment previous row
#  !!!         #return ''
##################################
def module_path():
	if hasattr(sys, "frozen"):
		return os.path.dirname(
			unicode(sys.executable, sys.getfilesystemencoding())
		)
	return os.path.dirname(unicode(__file__, sys.getfilesystemencoding()))+'\\'
	#return ''

	
########################################
# Name       : __request
# Purpose    : Sends http request for the given url
# Usage      : html = __request(url_par)
# Parameters : url - url address
#              url_par - additional url parameters
# Returns    : conneection string or blank if fail
########################################
def __request(url, url_par):
	
	#encode parameters if exist
	if url_par:
		encoded_params=urllib.urlencode(url_par)
		request=url%(encoded_params)
	else:
		#create final request string
		request=url
	
	# print it to the screen to be sure that path is correct 
	# and possibility to use that path to recheck connection
	#print 'Getting url...'
	print request
	
	#we will try to reconnect up to 4 times
	attempts=0
	while attempts<4:
		# Connect
		try:
			conn=urllib2.urlopen(request, None)
			attempts+=1
			break
		except urllib2.HTTPError, error: # simple exception handle
			print "Connection ERROR. Reconnecting after 4 sec..."
			conn=''
			time.sleep(4)
			attempts+=1
			
		print '*** Unable to connect after '+str(attempts)+' tries!'
	
	# If we cannot to connect - print current symbol to skip log
	if attempts>=4:
		skip=open(module_path()+'skiped.log', 'a')
		skip.write(str(url_params['s']))
		skip.write("\n")
		skip.close()
		conn=''
		
	return conn

	
########################################
# Name       : getData
# Purpose    : Gets data
# Usage      : getData()
# Parameters : link - url can be set manually, call with 'None' if need to use url_params
# Returns    : Nothing
########################################
def getData(link):
	
	if link: # need to get all the dates, we know urls..
		html=__request(link, None)
	else:
		# go to the target url
		# ex. http://finance.yahoo.com/q/os?s=AAPL&m=2014-01
		html=__request('http://finance.yahoo.com/q/os?%s', url_params)
		
		#print smart info row 
		print str('-'*60)+'\n'+'Current symbol: '+str(url_params['s'])+' | Current date: '+str(curDate.strftime("%Y-%m"))+'\n'+str('-'*60)
	
	# open csv file where results will be stored
	g=open(module_path()+'res.csv', 'a')
	
	# get tables using BeautifulSoup parser
	#print 'Getting tables...'
	soup=BeautifulSoup(''.join(html)) #parse html source
	table=soup.find('table', border="0", cellpadding="3", cellspacing="1", width="100%") # get required table
	if not table:
		print '*** It seems search criterio "'+str(url_params['s'])+'" did not give an exact result. Processing of this symbol was skipped. Please clarify the name and rerun the script!!!!!'
		skip=open(module_path()+'skiped.log', 'a')
		skip.write(str(url_params['s']))
		skip.write("\n")
		skip.close()
		return
		
	rows=table.findAll('tr') # get all rows in this table
	if len(rows)<2: # if no rows just skip the table and print dashes
		print '*** Table for "'+str(url_params['s'])+'" is empty!'
		g.write(url_params['s']+('--,'*13)+url_params['m']) 
		g.write("\n")
		return
	
	#we also need expiration date
	#ex. Options Expiring Friday, July 20, 2012
	exDatePattern=re.compile(r'''Options Expiring \w{6,13}, \w{3,10} \d{2}, \d{1,4}''', re.IGNORECASE) 
	# get date pattern
	exDateStr=''.join(exDatePattern.findall(str(soup)))
	# remove 'Options Expiring'
	exDate=exDateStr[''.join(exDateStr).find(',')+1:].strip()
	
	#determine values
	vMonthTemp=str(exDate[:exDate.find(' ')]).strip()
	vDay=str(exDate[len(vMonthTemp):exDate.find(',')]).strip()
	vYear=str(exDate[exDate.rfind(',')+1:]).strip()

	months=['January', 'February', 'Mart', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
	
	if not vMonthTemp in months:
		# if we are here - something like "Options Expiring 1969-12-31" and blank table is present.
		# just put '---' and return
		g.write(str(curDate)+','+url_params['s']+(',--'*12)) 
		g.write("\n")
		return

	vMonth=str(months.index(vMonthTemp)+1)
		
	expirDate=vMonth+'/'+vDay+'/'+vYear
	
	# go get table columns and real values
	
	# now we in table
	for tr in rows[2:]: # skip header rows
		cols=tr.findAll('td') # find all columns in table
		row=[] # clear temporary list that stores row for csv
		tempCall=[]
		tempPut=[]
		tdCount=0
		for td in cols: # for each 'cell' in column
			tdCount+=1
			#two types of values - one simple text, other - under '<b>' tag
			# replace commas with dots to make correct numbers (in the source table could be both commas and dots..)
			if tdCount<8: # call side
				if td.find('b'): 
					tempCall.append((td.find('b').text).replace(',', '.'))
				else:
					tempCall.append(td.find(text=True).replace(',', '.'))
			elif tdCount==8: #strike
				if td.find('b'): 
					strike=(td.find('b').text).replace(',', '.')
				else:
					strike=td.find(text=True).replace(',', '.')
					
			elif tdCount>8: # put side
				if td.find('b'): 
					tempPut.append((td.find('b').text).replace(',', '.'))
				else:
					tempPut.append(td.find(text=True).replace(',', '.'))

		#----------------- CALL

		#write rows in format
		#date,instrument,option_symbol,symbol,expiration,type,strike_price,last_price,change,bid,ask,volume,open_interest
		row.append(curDate.strftime("%m/%d/%Y")) 	# date
		row.append(tempCall[0][:3])					# instrument
		row.append(tempCall[0])						# option_symbol
		row.append(tempCall[0][:3])					# symbol
		row.append(expirDate)						# expiration
		row.append('Call')							# type
		row.append(strike)							# strike
		
		row.append(','.join(tempCall[1:]))

		#now we've got all the values!
		g.write(','.join(row)) # ...and all the values for it
		g.write("\n") # end the string and continue loop until rows exist
		
		#----------------- PUT
		
		row=[] # clear temporary list that stores row for csv
			
		#write rows in format
		#date,instrument,option_symbol,symbol,expiration,type,strike_price,last_price,change,bid,ask,volume,open_interest
		row.append(curDate.strftime("%m/%d/%Y")) 	# date
		row.append(tempPut[0][:3])					# instrument
		row.append(tempPut[0])						# option_symbol
		row.append(tempPut[0][:3])					# symbol
		row.append(expirDate)						# expiration
		row.append('Put')							# type
		row.append(strike)							# strike
		
		row.append(','.join(tempPut[1:]))

		#now we've got all the values!
		g.write(','.join(row)) # ...and all the values for it
		g.write("\n") # end the string and continue loop until rows exist
	return True

#####################################################################################################
#                                       START MAIN PROGRAM
#####################################################################################################

# we need to parse parameters
parser=optparse.OptionParser()
parser.add_option('-d', '--date', dest='date', help='Expiring date (OPTIONAL: default is today)') # date
parser.add_option('-s', '--symbol', dest='symbol', help='Symbol name: AAPL, GOOG etc.') # symbol
parser.add_option('-e', '--exact', dest='exact', help='Provide list of <SYMBOL> <DATE> in file') # list to extract exactly
parser.add_option('-a', '--alldates', dest='alldates', help='Provide list of <SYMBOL> in file') # list to extract all dates
	
# we've got them
options, args=parser.parse_args()

# determine work mode by options

# --alldates option has the highest priority
# --exact is the second
# --symbol becomes required option if none of above provided
if not options.alldates: 
	if not options.exact: 
		if not options.symbol: # symbol is required 
			parser.error('--symbol required')
			
if not options.date: # date can be omited
	curDate=date.today()
else:
	DateTemp=map(int, str(options.date)[1:-1].split('-'))
	curDate=date(DateTemp[0], DateTemp[1], 1)
			
parsSymbol=[] # define list for sybmols from options file
parsDate=[] # the same list for dates form file

# we will validate options file via simple regex
parsPattern=re.compile(r'''^\w{1,5} \d{4}-\d{2}$''', re.IGNORECASE) # both symbol and date
parsSymbolPattern=re.compile(r'''^\w{1,5}''', re.IGNORECASE) # only symbol
parsDatePattern=re.compile(r'''\d{4}-\d{2}$''', re.IGNORECASE) # only date

url_params={} # define url dict

# if --alldates or --exact given - read list from file and stop futher options parse
if options.alldates or options.exact: 
	
	# Store filename
	if options.alldates:
		filename=options.alldates[1:-1]
	elif options.exact:
		filename=options.exact[1:-1]
	print module_path()+filename

	# check that file is present
	if not os.path.exists(module_path()+filename):
		print '*** OptionFileParse: input file not found! Provide file or remove parameter form .py call!'
		sys.exit()
	else:
		
		print 'The file is found' # well.. we've got a file...
		parsFile=open(module_path()+filename) # ..opening
		while True: # read all the lines 
			
			line=parsFile.readline() # it's one row
			if not line: break # make sure it's not blank, leave if so and break while true loop
			
			# parse options file
			if parsPattern.findall(line): # try to find both symbol and date
				parsSymbol.append(','.join(parsSymbolPattern.findall(line)))
				parsDate.append(','.join(parsDatePattern.findall(line)))
			elif parsSymbolPattern.findall(line): # maybe date exist..
				parsSymbol.append(','.join(parsSymbolPattern.findall(line)))
				#revertDate = str(date.today())[2:7].replace('-','')
				#parsDate.append(revertDate)
				parsDate.append(str(curDate))
			else: # nothing found or incorrect. Print error mesaage and stop
				print 'Line in file is not in format "<symbol> <date>" or "<symbol>" ex. "GOOG 1208" (SYMBOL YYMM), "TSL" (SYMBOL). Please provide the correct file or remove it. Exit'
				sys.exit()
		#close file, it's not needed anymore
		parsFile.close()
		
	#print obtained info from file
	print parsSymbol, parsDate
	print str('='*60)+'\n'+str(' '*60)+' Found '+str(len(parsSymbol))+' lines '+'\n'+str('='*60)+'\n'

	i=0 # counter to print current option's set
	# start processing of given options
	for temp in parsSymbol: # get each option set
		# we should translate given options to the correct address parameters
		if parsDate[i]: # translate date
			#should be : ex. 2014-01

			DateTemp=map(int, str(parsDate[i]).split('-'))
			curDate=date(DateTemp[0], DateTemp[1], 1)
		else: # default is today if omited
			curDate=date.today()
		
		# symbol is already good. I've got all necessary url parameters!
		url_params['s']=parsSymbol[i]
		
		#print for review
		print str('='*20)+' Processing line ('+str(i+1)+' of '+str(len(parsSymbol))+') '+str('='*20)
		
		# now we are ready to get info
		if options.exact:
			getData(None) # just get data for current line
		elif options.alldates: # wee need to go thru all links
			links=[]
			# go to the target page to know date's links
			html=__request('http://finance.yahoo.com/q/os?s='+str(parsSymbol[i]), None)
			soup=BeautifulSoup(''.join(html)) #parse html source
			
			#get all links containing reference to the date, ex. "/q/os?s=AAPL&amp;m=2014-01"
			linkPattern=re.compile(r'''"/q/os\?s=.*?\&amp;m=\d{4}-\d{2}"''')
			links=linkPattern.findall(str(soup))
			
			print str('-'*10)+'  Getting date (1 of '+str(len(links)+1)+')  '+str('-'*10)
			curSymb=str(parsSymbol[i])
			getData('http://finance.yahoo.com/q/os?s='+curSymb)

			k=1
			for link in links:
				k+=1
				print str('-'*10)+'  Getting date ('+str(k)+' of '+str(len(links)+1)+')  '+str('-'*10)
				
				getData('http://finance.yahoo.com'+str(link)[1:-1].replace('&amp;', '&'))
		i+=1
		
else:

	# Setup URL params from options
	if options.date:
		url_params['m']=options.date
	if options.symbol:
		url_params['s']=options.symbol
	
	# we have the parameters for url. Start to get data
	getData(None)
	
# Thats all we done!
print 'Done!'

#####################################################################################################
#                                           END OF FILE                                             #
#####################################################################################################

