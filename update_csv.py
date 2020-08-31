import tabula
import numpy as np
import re
import pandas as pd
from matplotlib import pyplot as plt
import os


def extract_pdf(filepath):
    test_df = tabula.read_pdf(filepath,pages=1,silent=True,area=[100,0,1200,1000])[0].fillna(" ")
    test_df.columns = np.arange(len(test_df.columns))
    
    if filepath in ['presslogs/log_2019_10_04.pdf','presslogs/log_2019_10_06.pdf']:#handling those two with weird columns
        test_df = test_df.rename({0:1, 1:0},axis=1)
    
    start_rows = test_df[test_df[0].apply(lambda x: re.match('([0-9]{4}-[0-9]{1,2}-[0-9]{1,2})',str(x)) is not None)].index
    start_rows = list(start_rows)
    
    if len(start_rows) == 0:
        return pd.DataFrame([])

    #merging rows together
    processed_df = pd.DataFrame([test_df.loc[start_rows[i]:start_rows[i+1]-1].apply(lambda x: " ".join(x)).apply(lambda x: x.strip()) for i in range(0,len(start_rows)-1)])
    if len(processed_df)==0:
        final_index = 0;
        processed_df = pd.DataFrame([],columns = test_df.columns)
    else:
        final_index = max(processed_df.index)+1;
    processed_df.loc[final_index] = test_df.loc[start_rows[-1]:].apply(lambda x: " ".join(x)).apply(lambda x: x.strip()) #adding final row

    #rename columns
    processed_df.columns = test_df.loc[0:start_rows[0]-1].apply(lambda x: " ".join(x)).apply(lambda x: x.strip())

    processed_df['Filename'] = os.path.basename(filepath)
    
    return processed_df

def replace_multiple_space(string):
    return re.sub(' +', ' ', string)

def unjumble_address(string):
    '''for AddressComments'''
    string= str(string)
    pattern = '[A-Z][a-z]'
    result = re.search(pattern,string)
    
    if result is None: #if there's no lowercase stuff, then it's just an address
        return string,''
    
    address_string = string[0:result.start()]
    
    string = string[result.start():]
    
    extraction_string = "[(]?[A-Z]{2,}[)]?"
    while bool(re.search(extraction_string,string)) is True:
        result = re.search(extraction_string,string)
        start, end = result.start(), result.end()
        string = string[0:start] + string[end:]
        
        address_string += result.group()
        
    string = replace_multiple_space(string.strip())
    address_string = replace_multiple_space(address_string)
        
    return address_string, string

def process_row(row):
    #starting empty row
    results = pd.Series([],dtype=object)
    
    #occurred
    if 'DateTimeOccurred' in row.index:
        results['DateTimeOccurred'] = replace_multiple_space(row['DateTimeOccurred'])
        
    #status
    if 'Disposition' in row.index:
        results['Status'] = row['Disposition']
    
    #reported
    if "DateTimeReported" in row.index:
        results['DateTimeReported'] = replace_multiple_space(row['DateTimeReported'])
        
    if "DateTimeIncTypeReported" in row.index:#extract reported time and code
        data = row['DateTimeIncTypeReported']
        regex_pattern = '([0-9]{4}-[0-9]{2}-[0-9]{2})(.*)([0-9]{2}:[0-9]{2}:[0-9]{2})(.*)'
        
        
        date, str1, time, str2 = re.match(regex_pattern,data).group(1,2,3,4)
        
        #processing the code string
        code_string = (str1 + str2) #combining parts
        code_string = code_string.replace('NO REPORTS OF RESIDENTIAL FIRES','').replace('.','') #removing fire stuff
        code_string = code_string.strip()
        code_string = replace_multiple_space(code_string)
        
        #processing datetime
        datetime = "{date} {time}".format(**locals())
        
        #adding to results
        results['DateTimeReported'] = datetime
        results['Code'] = code_string
    
    #IncType
    if 'IncType' in row.index:
        results['Code'] = row['IncType']
        
    #address
    if 'Address' in row.index:
        results['Location'] = replace_multiple_space(row['Address'])
    
    #Comments
    if 'Comments' in row.index:
        results['Comments'] = replace_multiple_space(row['Comments'])
        
    #AddressComments
    
    if 'AddressComments' in row.index:
        address_string, comment_string = unjumble_address(row['AddressComments'])
        results['Location'] = address_string
        results['Comments'] = comment_string
        
    #filename
    results['Filename'] = row['Filename']
    
    
    
    return results

def process_df(df):
    df = df.rename(lambda x: x.replace(' ','').replace('&',''), axis=1)
    if '' in df.columns:
        df = df.drop('',axis=1) #removing columns with empty names
        
        
    df = df.apply(process_row,axis=1)
    return df


def update():
    #figure out which PDFs have not been processed yet
    processed_pdfs = [line.strip() for line in open('processed_logs.txt','r')]
    unprocessed_pdfs = [file for file in os.listdir('presslogs') if file not in processed_pdfs and file.endswith('.pdf')]

    #seeing if there are any unprocessed_pdfs
    if len(unprocessed_pdfs) == 0:
        print('Nothing to process.')
        return

    #processing PDFs

    df = pd.concat(  [process_df(extract_pdf(os.path.join('presslogs',pdf))) for pdf in unprocessed_pdfs ])
    
    
    #appending to logs csv
    df.to_csv('full_logs.csv',index=False,mode='a',header = False)
    
    #adding processed logs to list
    f=open("processed_logs.txt", "a+")
    for pdf in unprocessed_pdfs:
        f.write('\n{0}'.format(pdf))
    f.close()
    
    #printing unprocessed 1
    print(unprocessed_pdfs)
    
    return df



if __name__ == "__main__":
    #changing path
    path = os.path.dirname(__file__)
    if path == '': path = '.'
    os.chdir(path)
    #updating
    update()