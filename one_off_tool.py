# # one off tool
# from itertools import cycle

# # Count how many lines are in the csv file
# count = 0
# with open('python_repos.csv', 'r') as f:
# 	for line in f:
# 		count += 1

# # Split up the file into 4
# f1 = open('python_repos_1.csv', 'w')
# f2 = open('python_repos_2.csv', 'w')
# f3 = open('python_repos_3.csv', 'w')
# f4 = open('python_repos_4.csv', 'w')

# header = 'user,repo_name,num_bytes\n'
# f1.write(header)
# f2.write(header)
# f3.write(header)
# f4.write(header)

# with open('python_repos.csv', 'r') as f:
# 	for line, f_out in zip(f, cycle([f1, f2, f3, f4])):
# 		f_out.write(line)

# f1.close()
# f2.close()
# f3.close()
# f4.close()


with open('python_repos2.csv', 'w') as f_out:
	f1 = open('python_repos_1_out.csv', 'r')
	f2 = open('python_repos_2_out.csv', 'r')
	f3 = open('python_repos_3_out.csv', 'r')
	f4 = open('python_repos_4_out.csv', 'r')

	# Write header
	f_out.write('user,repo_name,num_bytes,stars,fork\n')

	# Skip header
	_, _, _, _ = f1.readline(), f2.readline(), f3.readline(), f4.readline()
	
	f_out.write(f1.read())
	f1.close()
	f_out.write(f2.read())
	f2.close()
	f_out.write(f3.read())
	f3.close()
	f_out.write(f4.read())
	f4.close()
