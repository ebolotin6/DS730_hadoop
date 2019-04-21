# Project 1: MapReduce using Python

### Problem 1: Finding Big Spenders

Assume you work for a large business and have access to all orders made in any given time period (download [orders.csv](http://www.uwosh.edu/faculty_staff/krohne/ds730/orders.csv)).

We will use this exact csv file when testing your code. The column headings are fairly self-explanatory. Your company wants you to find the big spenders for each month and country so they can market more heavily to them. Your goal is this: for each month/country combination, display the customerID of the top spender (i.e. the sum of how much that customer spent) for that month/country combination. The amount spent in each row is determined by multiplying the Quantity by the UnitPrice. 

A few caveats:

* The InvoiceDate is in Month/Day/Year format.
* An InvoiceNo that starts with a C is a return. You must ignore these rows.
* A row may not have a CustomerID. These rows must be ignored.
* I am *not* looking for month/year/country combinations here. There are two years worth of data and I am only interested in the month and country.

This final output file should contain the following data in this format: 

**Month,Country:CustomerID**

The month must be a two-digit number (i.e., 01, 02, ..., 09, 10, 11, 12) and must be separated from the Country using a comma. The Month,Country portion is separated by the CustomerID using a colon. If there is a tie, you must print out all customers who tied separating the CustomerID’s by a comma.

### Solution 1: Mapper

```python
#!/usr/bin/env python

import sys

def main(argv):
    # read in first line
    line = sys.stdin.readline()
    # if line starts with header row, skip it (get next line)
    if line.startswith('InvoiceNo'):
        line = sys.stdin.readline()
    # loop through remaining lines
    for line in sys.stdin:
        # split line into list at commas
        row = line.split(',')
        # check if invoice starts with upper or lowercase C
        invoice_test = row[0].startswith(('C','c'))
        # get month
        month = row[4].split("/",1)[0]
        # strip whitespace from country
        country = row[7].strip()
        # get other column values
        customer_id = row[6]
        quantity = int(row[3])
        unitprice = float(row[5])
        amount_spent = quantity * unitprice
        # if customer_id is not empty and invoice_test is false, print output
        if customer_id != '' and invoice_test is False:
            print('%s,%s,%s\t\t\t%s' % (month, country, customer_id, amount_spent))

if __name__ == "__main__":
    main(sys.argv)
```

### Solution 1: Reducer

```python
#!/usr/bin/env python

import sys
import operator

def main(argv):
    # read in first line
    for line in sys.stdin:
        # get line values
        prev_month, prev_country, prev_customer_id, prev_amount_spent =  _get_row_values(line)
        # define most spent and top spender (customer id)
        top_spent = prev_amount_spent
        top_spender = prev_customer_id
        
        # loop through every line in standard input
        for line in sys.stdin:
            # get line values
            current_month, current_country, current_customer_id, current_amount_spent =  _get_row_values(line)

            # check if current country and month vary from previous country/month
            if prev_country == current_country and prev_month == current_month:
                # check if prev customer id is equal to current customer id
                if prev_customer_id == current_customer_id:
                    prev_amount_spent += current_amount_spent
                
                # if they are not equal to eachother
                elif prev_customer_id != current_customer_id:
                    # then check to see if incremented amount spent is greater than top spent
                    if top_spent < prev_amount_spent:
                        # if so, set top spent to (new) amount spent, and save top spender
                        top_spent = prev_amount_spent
                        top_spender = prev_customer_id

                    # if there is a tie, then append customer_id to top_spender
                    elif top_spent == prev_amount_spent:
                        top_spender = top_spender + "," + prev_customer_id
                    
                    prev_amount_spent = current_amount_spent
                    prev_customer_id = current_customer_id

            # if previous country/month are not the same as current country/month
            else:
                # check if incremented amount spent is greater than top_spent
                if top_spent < prev_amount_spent:
                    # set new top spent/spender
                    top_spent = prev_amount_spent
                    top_spender = prev_customer_id

                # if there is a tie
                elif top_spent == prev_amount_spent:
                    # append tied customer_id to top spender
                    top_spender = top_spender + "," + prev_customer_id

                # print month, country, and top spender
                print('%s,%s:%s' % (prev_month, prev_country, top_spender))
                
                # update variables for next iteration
                prev_amount_spent = current_amount_spent
                prev_country = current_country
                prev_customer_id = current_customer_id
                prev_month = current_month
                top_spender = prev_customer_id
                most_spent = prev_amount_spent

        # print final line
        print('%s,%s:%s' % (prev_month, prev_country, top_spender))

# function performs activites related to line splitting & value formatting
def _get_row_values(line):
    try:
        row = line.split("\t")
        del row[2]
        del row[1]
        key = row[0].split(",")
        value = row[1].strip("\n")
        month = key[0]
        # if current month is single digit, prepend 0
        if len(month) == 1:
            month = '0' + month

        country = key[1]
        customer_id = key[2]
        amount_spent = float(value)
        return(month, country, customer_id, amount_spent)

    except Exception as error:
        print(error)
# Note there are two underscores around name and main
if __name__ == "__main__":
    main(sys.argv)
```

### Problem 2: Words with exact same vowels

This problem is similar to the problem we worked on in lecture with a small twist. Instead of printing out how many times a word appears in the file, you want to print out how many words have the exact same type of vowels. For this problem, only the number of vowels matters and the case does not matter (i.e cat is the same as CAt). A vowel is any letter from this set {a,e,i,o,u,y}. A word is any sequence of characters that does not contain whitespace. Whitespace is defined as: space, newline or tab. All of the following are 1 single word:

* cats
* c@ts
* ca7s
* cat’s.and:d0gs!

The output will be the vowel set, followed by a colon, followed by the number of words that contained exactly the vowel set. The output will have one answer per line (see example below).

#### Example:
‘hello’ and ‘pole’ both contain exactly 1 e and exactly 1 o. The order of the vowels from the original input word does not matter. Imagine the following example:

```
hello    moose
pole cccttt.ggg
```

We would end up with the following output:
```
:1
eo:2
eoo:1
```

The format should be as seen above: the vowels on each line are in alphabetical order, followed by a colon, then followed by the number of words that contained exactly those vowels. If there are words with no vowels, nothing is printed before the colon.

### Solution 2: Mapper

```python
#!/usr/bin/env python

import sys

def main(argv):
    # define lowercase and uppercase vowels
    lower_vowels = ['a', 'e', 'i', 'o', 'u', 'y']
    upper_vowels = map(lambda v: v.upper(), lower_vowels)
    # combine lower & upper case vowels into one list
    vowels = list(zip(lower_vowels, upper_vowels))
    for line in sys.stdin:
        # split the line into a list at any occurrence white space
        line = line.split()
        # loop through every word in a line
        for word in line:
            # and keep track of vowels present for every word
            vowels_present = ''
            # loop through list of vowels
            for lower_vowel, upper_vowel in vowels:
                # for every occurrence of a vowel, add to our list of vowels_present
                if lower_vowel in word:
                    vowels_present += lower_vowel * word.count(lower_vowel)
                if upper_vowel in word:
                    vowels_present += upper_vowel * word.count(upper_vowel)
            # print present vowels
            print('%s:%s' % (vowels_present, 1))
if __name__ == "__main__":
    main(sys.argv)
```

### Solution 2: Reducer

```python
#!/usr/bin/env python

import sys

def main(argv):
	# set initial variables
	first_vowel = None
	first_count = None
	# iterate through standard input
	for line in sys.stdin:
		# split line by colon
		line = line.split(':')
		# define first key (vowel) and value (count)
		next_vowel = line[0]
		next_count = int(line[1].strip())
		# if initial vowel and current vowel match, increment count
		if first_vowel == next_vowel:
			first_count += next_count
		else:
			# if first_vowel is not none, output key/val count
			if first_vowel is not None:
				print('%s:%s' % (first_vowel, first_count))
			# reset vars
			first_vowel = next_vowel
			first_count = next_count
	# if end of file, print results
	if next_vowel == first_vowel:
		print('%s:%s' % (first_vowel, first_count))
if __name__ == "__main__":
	main(sys.argv)
```

### Problem 3: Discovering Contacts

On many social media websites, it is common for the company to provide a list of suggested contacts for you to connect with. Many of these suggestions come from your own list of current contacts. The basic idea behind this concept being: I am connected with person A and person B but not person C. Person A and person B are both connected to person C. None of my contacts are connected to person D. It is more likely that I know person C than some other random person D who is connected to no one I know. For this problem, all connections are mutual (i.e. if A is connected to B, then B is connected to A). In this problem, you will read in an input file that is delimited in the following manner:

```
PersonA : PersonX PersonY PersonZ PersonQ
PersonB : PersonF PersonY PersonX PersonG PersonM 
...
```

For example, the person to the left of the colon will be the current person. All people to the right of the colon are the people that the current person is connected to. All people will be separated by a single space. In the example above, PersonA is connected to PersonX, Y, Z and Q. In all inputs, all people will be replaced with positive integer ids to keep things simple. The following is a sample input file:

```
1 : 3 5 8 9 10 12 
2 : 3 4 7 6 13
3 : 9 11 10 1 2 13 
4 : 2 5 7 8 9
5 : 4 1 7 11 12
6 : 2 9 8 10
7 : 5 2 4 9 12 
8 : 1 6 4 11
9 : 12 1 3 6 4 7 
10 : 1 3 6 11 
11 : 3 5 10 8 
12 : 1 7 5 9
13 : 2 3
```

The ordering of people on the right hand side of the input can be in any order. Your goal is this: you must output ​potential contacts based on the following 2 criteria:
* Someone who **might** be someone you know. For someone to be suggested here, the person must not currently be a connection of yours and that person must be a connection of exactly 2 or 3 of your current connections. For example, consider person 2 in the above example. Person 2 is connected with 3, 4, 6, 7 and 13. Person 4 is connected to 8, person 6 is connected to 8, person 3 is not connected to 8, person 7 is not connected to 8 and person 13 is not connected to 8. Therefore, person 2 has two connections (4 and 6) that are connected to 8 and person 2 is not currently connected to 8. Therefore, person 2 might know person 8.
* Someone you **probably** know. For someone to be suggested here, the person must not currently be a connection of yours and that person must be a connection of 4 or more of your current connections. For example, consider person 2 in the above example. Person 2 is connected with 3, 4, 6, 7 and 13. Person 4 is connected to 9, person 6 is connected to 9, person 3 is connected to 9 and person 7 is connected to 9. Therefore, person 2 has at least four connections that are connected to 9 and person 2 is not currently connected to 9. Therefore, person 2 probably knows person 9.

As a concrete example from the above sample input, this would be the sample output:
```

1:Might(4,6,7) Probably(11) 
2:Might(5,8,10) Probably(9) 
3:Might(4,5,6,7,8,12) 
4:Might(1,3,6,11,12) 
5:Might(2,3,8,10) Probably(9) 
6:Might(1,3,4,7,11) 
7:Might(1,3,6) 
8:Might(2,3,5,9,10) 
9:Might(8,10) Probably(2,5) 
10:Might(2,5,8,9) 
11:Might(4,6) Probably(1) 
12:Might(3,4)
13:
```

### Solution 3: Mapper

```python
#!/usr/bin/env python

import sys

def main(argv):
    all_people = []
    # read from stdin stream
    for line in sys.stdin:
        # split string by space-colon-space
        line = line.split(" : ")
        # convert id to int
        person = int(line[0])
        connections = line[1].strip("\n")
        connections = list(map(int, line[1].split()))
        # '%02d %s' % (person, connections)
        for connection in connections:
            print('%s\t%s' % (person, connection))

if __name__ == "__main__":
    main(sys.argv)
```

### Solution 3: Reducer

```python
#!/usr/bin/env python

import sys
import collections

def main(argv):
    # read in first std input
    line = sys.stdin.readline()
    first_person_connections = []
    # split the line into first person and their connections
    first_person, first_connection = _split_line(line)
    # append first connection to list
    first_person_connections.append(first_connection)
    # create container for all connections
    all_connections = []
    
    # loop through rest of standard input
    for line in sys.stdin:
        # get values for each line
        second_person, second_connection = _split_line(line)

        # if person on first line is same as person on second line, append connections
        if first_person == second_person:
            first_person_connections.append(second_connection)
        
        # else, append to total connections and reset variables for next line
        else:
            _update_total_connections(first_person, first_person_connections, all_connections)
            first_person_connections = []
            first_person = second_person
            first_person_connections.append(second_connection)

    # update total connections for final line
    _update_total_connections(first_person, first_person_connections, all_connections)
    # print(all_connections)
    
    # loop through total (all) connections, rotating the list on each iteration via generator    
    for people in _rotate_list(all_connections):
        # get first person id
        first_person_id = people[0][0]
        
        # get first person connections
        first_person_connections = people[0][1:]
        
        # set up lists to track might_know vs probably_know
        might_know = []
        probably_know = []

        # loop through second person connections
        for person in people[1:]:
            second_person_id = person[0]
            second_person_connections = person[1:]
            # set variable to keep track of how many people in the second person's list are also in the first person's list
            num_people_known = 0

            if second_person_id not in first_person_connections:
                # loop through the connections of the second person
                for connection in second_person_connections:
                    # increment num_people_known if mutual acquaintences
                    if connection in first_person_connections:
                        num_people_known += 1

                # if the 2nd person has exactly 2 or 3 shared connections with the first person, append to might_know
                if num_people_known in [2, 3]:
                    # then append the 2nd person's acquaitenances to the might_know list
                    might_know.append(second_person_id)
                
                # if the 2nd person has 4 or more shared connections with the first person
                elif num_people_known >= 4:
                    # then append the 2nd person's acquaitenances to the probably_know list
                    probably_know.append(second_person_id)

        # logic to print information
        if len(might_know) != 0 and len(probably_know) != 0:
            might_know, probably_know = _convert_to_str(might_know, probably_know)
            print('%s:Might(%s) Probably(%s)' % (first_person_id, might_know, probably_know))
        
        elif len(might_know) != 0 and len(probably_know) == 0:
            might_know = _convert_to_str(might_know)
            print('%s:Might(%s)' % (first_person_id, might_know))
        
        elif len(might_know) == 0 and len(probably_know) != 0:
            probably_know = _convert_to_str(probably_know)
            print('%s:Probably(%s)' % (first_person_id, probably_know))
        
        else:
            print('%s:' % (first_person_id))

# function to rotate all_connections list
def _rotate_list(all_connections):
    i = 0
    # for each person in the list
    for person in all_connections:
        # creating object to rotate
        new_list = collections.deque(all_connections)
        new_list.rotate(i)
        # increment rotation amount
        i -= 1
        yield list(new_list)

# function to update all_connections list
def _update_total_connections(person, connections, all_connections):
    connections.insert(0, person)
    all_connections.append(connections)

# function to convert information before printing
def _convert_to_str(might_know = None, probably_know = None):
    try:
        if might_know is not None and probably_know is not None:
            might_know = sorted(might_know)
            might_know = ",".join(str(value) for value in might_know)
            probably_know = sorted(probably_know)
            probably_know = ",".join(str(value) for value in probably_know)
            
            return(might_know, probably_know)
        
        elif might_know is not None and probably_know is None:
            might_know = sorted(might_know)
            might_know = ",".join(str(value) for value in might_know)
            
            return(might_know)
        
        elif might_know is None and probably_know is not None:
            probably_know = sorted(probably_know)
            probably_know = ",".join(str(value) for value in probably_know)
            
            return(probably_know)
    
    except Exception as error:
        return(error)

# function to split lines
def _split_line(line):
    line = line.split("\t")
    person = int(line[0])
    connection = int(line[1])
    return(person, connection)

if __name__ == "__main__":
    main(sys.argv)
```