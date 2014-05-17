import re
import json
import boto
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import StringIO
#open Amazon S3 Bucket
conn = S3Connection("", '')#already made inactive
mybucket = conn.get_bucket('hadoop-netfish')
#read all MapReduce result files
fp=open("tweettrend.txt","w")

for i in range(0,3):
        key=mybucket.get_key("outputtrend/part-0000"+str(i))
        key_string=str(key.key)
        fp.write(key.get_contents_as_string())
fp.close()
#covert list of wordcounts to datatable
f=open("tweettrend.txt")
f_result=open("tweettrendtable.txt","w")
pattern=re.compile(r"(.*?)\t(.*?)$")#file format: word \t {time1:count1, time2:count2}
pattern_num=re.compile(r"(\d) (\d\d) (\d\d)")#format of time in file: m dd hh
#convert date to int
def convert_to_int(date):
        result=pattern_num.match(date)
        month,day,hour=int(result.group(1)),int(result.group(2)),int(result.group(3))
        if month==5:
                time=10*24+hour-19
        else:
                time=(day-21)*24+hour-19
        return time
#the last hour in dataset.
max_int=convert_to_int("5 01 11")
line_num=0#how many keywords are there in dataset
#read file to list
trend=[]
for l in f:
        result=pattern.match(l)
        if result:
                line_num+=1
                word=result.group(1)
                current_trend=json.loads(result.group(2)).items()#itemize the dict
                trend.append((word,current_trend))
                
result_table= [[0 for x in range(0,max_int+1)] for y in range(0,line_num)]#data table
result_title=[0 for x in range(0,line_num)]#word list
#column names(hour from start time) as the first row of the table
title="\"word\","
for x in range(0,max_int+1):
        title+="\""+str(x)+"\""
        if x<max_int:
                title+=","
title+="\n"
f_result.write(title)
#write data to table
#data: word,count0,count1,.....\n
for i,word in enumerate(trend):
        result_title[i]=word[0]
        for term in word[1]:
                result_table[i][convert_to_int(term[0])]=term[1]
#write table to file
for i in range(0,line_num):
        line=result_title[i]+","
        for j in range(0,max_int+1):
                line+=str(result_table[i][j])
                if j<max_int:
                        line+=","
        line+="\n"
        f_result.write(line)
        
f.close()
f_result.close()
result_table_normalized= [[0 for x in range(0,max_int+1)] for y in range(0,line_num)]
total_word_sum=[0 for x in range(0,line_num)]
#normalize table to make words with different frequency comparable
for y in range(0,line_num):
        total_count=sum(result_table[y])
        for x in range(0,max_int+1):
                result_table_normalized[y][x]=result_table[y][x]/float(total_count)
#the similarity between different words        
def distance(vector1,vector2):
        sqr_dist=0
        for i in range(0,max_int):
                if max(vector1[i]+vector1[i+1],vector2[i]+vector2[i+1])>0:
                        sqr_dist+=abs(vector1[i]+vector1[i+1]-vector2[i]-vector2[i+1])**2/float(max(vector1[i]+vector1[i+1],vector2[i]+vector2[i+1]))
        return sqr_dist
      
THRESHOLD=1
#convert raw data to google-chart compatible array
list_table=range(0,line_num)
result_array=[[str(i)] for i in range(0,max_int+1)]
result_array.insert(0,['hour'])
for i in list_table:
        result_array[0].append(result_title[i])
        for j in range(0,max_int+1):
                result_array[j+1].append(result_table[i][j])
f_todraw=open("datatodraw2.txt","w")
json.dump(result_array,f_todraw)
f_todraw.close()
key="finalresults/arrayofrawdata.txt"
k = Key(mybucket)
k.key = key
k.set_contents_from_filename("datatodraw2.txt")

#iteratively delete one word the closest pair of words

has_cut=True
similar_pairs=[]
while has_cut and len(list_table)>1:        
        has_cut=False
        min_dist=max_int
        min_pair=None
        #find the closest pair
        for indexi,i in enumerate(list_table):
            for indexj,j in enumerate(list_table):
                if i==j:
                        continue
                dist=distance(result_table_normalized[i],result_table_normalized[j])
                if dist<min_dist:
                    min_dist=dist
                    min_pair=[indexi,indexj]
        print min_dist,result_title[list_table[min_pair[0]]],result_title[list_table[min_pair[1]]]
        #if they are close enough, delete the one with lower wordcount, save the deletion record
        if min_dist<THRESHOLD:
            similar_pairs.append([result_title[list_table[min_pair[0]]],result_title[list_table[min_pair[1]]]])
            if total_word_sum[min_pair[0]]>total_word_sum[min_pair[1]]:
                del list_table[min_pair[1]]
            else:
                del list_table[min_pair[0]]
            has_cut=True
#convert processed data to google-chart compatible array  
f_eliminated=open("result_eliminated.txt","w")
result_array=[[str(i)] for i in range(0,max_int+1)]
result_array.insert(0,['hour'])
for i in list_table:
        result_array[0].append(result_title[i])
        line="\""+result_title[i]+","
        for j in range(0,max_int+1):
                result_array[j+1].append(result_table[i][j])
                line+=str(result_table[i][j])
                if j<max_int:
                        line+=","
        line+="\n"
        f_eliminated.write(line)
f_eliminated.close()

key="finalresults/mergedtrend.txt"
k = Key(mybucket)
k.key = key
k.set_contents_from_filename("result_eliminated.txt")

f_todraw=open("datatodraw.txt","w")
json.dump(result_array,f_todraw)
f_todraw.close()
key="finalresults/arrayofmergeddata.txt"
k = Key(mybucket)
k.key = key
k.set_contents_from_filename("datatodraw.txt")
word_group=[]
for pair in similar_pairs:
        word_group.append(set(pair))
has_merged=True
while has_merged:
        has_merged=False
        try:
                for i in range(0,len(word_group)-1):
                        group1=word_group[i]
                        for j in range(i+1,len(word_group)):
                                group2=word_group[j]
                                group=group1 | group2
                                if (len(group1)+len(group2))>len(group)>max(len(group1),len(group2)) or (len(group1)!=len(group2) and len(group)==max(len(group1),len(group2))):
                                        word_group[i]=group
                                        group1=group
                                        print word_group[j]
                                        del word_group[j]
                                        has_merged=True
                                        raise Exception
        except Exception:
                pass
similar_groups=[]
for wordset in word_group:
	similar_groups.append(list(wordset))
        
f_todraw=open("datatodraw3.txt","w")
json.dump(similar_groups,f_todraw)
f_todraw.close()
key="finalresults/mergerecord.txt"
k = Key(mybucket)
k.key = key
k.set_contents_from_filename("datatodraw3.txt")
