A. Top module/package names
	Regex to extract dependency name
	Drop if dependency name == name of a file/directory within the
		same repo
	Drop if there are duplicate dependencies within the same repo

B. Co-occurrences of modules across all repos
	For each file
		Create a set of modules
		Drop modules with name == name of a file/dir in same repo
		For each module
			increment Riak Map between this module and all other modules

C. Top file names
	Map: Extract the file name and do (filename, 1)
	ReduceByKey: sum
	Top: Top N filenames, key is the 2nd field of the tuple

D. How Go repos depend on one another
	Go
		Find dependencies within file that starts with 'github'
	NodeJS
		Look at package.json files to figure out dependencies
