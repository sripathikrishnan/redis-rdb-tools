#!/usr/bin/python

import sys

try:
	filename = sys.argv[1]
except IndexError:
	print "Usage %s filename [max_rows]" % sys.argv[0]
	sys.exit(1)

try:
	maxbr = long(sys.argv[2])
except IndexError:
	maxbr = None



br = 0
br_size_in_bytes = 0
br_max = 0

s_db   = {}
s_tipe = {}
s_hist = {}



s_hist_levels = []

for i in range(32 + 4):
	s_hist_levels.append(2 << i)



for line in open(filename):
	if br :
		if br % 1000 == 0 :
			print "\r%12d records processed" % br ,
	try:
		x = line.strip().split(",")
		(db, tipe, key, size_in_bytes, encoding, num_elements, len_largest_element) = x

		db		= int(db)
		size_in_bytes	= int(size_in_bytes)
	except ValueError :
		continue

	if db == "database" :
		continue
	
	# Global count
	
	br += 1
	br_size_in_bytes += size_in_bytes
	if br_max < size_in_bytes :
		br_max = size_in_bytes
	
	# Database
	
	try:
		s_db[db]["br"]			+= 1
		s_db[db]["size_in_bytes"]	+= size_in_bytes

		if s_db[db]["max"] < size_in_bytes :
			s_db[db]["max"]		= size_in_bytes
	except KeyError:
		s_db[db] = {
			"br"		:	1			,
			"size_in_bytes"	:	long(size_in_bytes)	,
			"max"		:	long(size_in_bytes)	,
		}

	# Key type

	try:
		s_tipe[tipe]["br"]		+= 1
		s_tipe[tipe]["size_in_bytes"]	+= size_in_bytes
		
		if s_tipe[tipe]["max"] < size_in_bytes :
			s_tipe[tipe]["max"]	= size_in_bytes
	except KeyError:
		s_tipe[tipe] = {
			"br"		:	1			,
			"size_in_bytes"	:	long(size_in_bytes)	,
			"max"		:	long(size_in_bytes)	,
		}
	
	# histogram
	
	for i in reversed(s_hist_levels) :
		if size_in_bytes > i :
			try:
				s_hist[i]["br"]			+= 1
				s_hist[i]["size_in_bytes"]	+= size_in_bytes
				
				if s_hist[i]["max"] < size_in_bytes :
					s_hist[i]["max"]	= size_in_bytes
			except KeyError:
				s_hist[i] = {
					"br"		:	1			,
					"size_in_bytes"	:	long(size_in_bytes)	,
					"max"		:	long(size_in_bytes)	,
				}
				
			break
	
	#print x
	
	if maxbr :
		if br >= maxbr :
			break



def table_head(title):
	tit      = "%12s : %12s : %6s : %12s : %6s :  %12s : %12s" % (
					"#"				, 
					"count"				,
					"count%"			,
					"size bytes"			,
					"size%"				,
					"avg size"			,
					"max size"
				)
	tit_line = "-" * 91

	print
	print
	print title
	
	print tit_line
	print tit
	print tit_line

def table_foot():
	tit_line = "-" * 91
	
	print tit_line
	print "%12s : %12d : %6.2f : %12d : %6.2f :  %12d : %12d" % (
					"total"				, 
					
					br				,
					100.00				,
					
					br_size_in_bytes		, 
					100.00				,
					
					br_size_in_bytes / br		, 
					br_max
				)



def table(title, m):	
	table_head(title)
	for i in sorted(m.keys()) :
		x = m[i]
		print "%12s : %12d : %6.2f : %12d : %6.2f :  %12d : %12d" % (
					i				,
					
					x["br"]				,
					x["br"] / float(br) * 100	,
					
					x["size_in_bytes"]		,
					x["size_in_bytes"] / float(br_size_in_bytes) * 100 ,
					
					x["size_in_bytes"] / x["br"]	, 
					x["max"]
				)
	table_foot()

table("Distribution by Database",	s_db	)
table("Distribution by key type",	s_tipe	)
table("Distribution by sizetype",	s_hist	)




