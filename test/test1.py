# Build and return a list
def firstn(n):
   num, nums = 0, []
   while num < n:
       nums.append(num)
       num += 1
#   print nums
   return nums

sum_of_first_n = sum(firstn(100))
#print sum_of_first_n

def firstng(n):
	num = 0
	while num < n:
		yield num 
		num += 1
sum_of_first_n = sum(firstn(50))

# list comprehension
doubles = [2*n for n in range(50)]
print doubles	

doubles1 = list(2*n for n in range(50)) 
print doubles1