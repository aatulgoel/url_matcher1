import pandas as pd
raw_url_list = '/a/s/d/f/g'.split('/')
s = pd.Series(['1,2','1,2','1,2'])
print(s)
# s['a'] =  "/".join(
#     ['XXXXXXXXXXXXXXXX' if x in s.iloc[index].split() else x for index, x in enumerate(raw_url_list, 0)])
print (s)

for index, j in enumerate(raw_url_list, 0):
    print("index = "+ str(index))
    k = s.iloc[index].split()
    print(k)



