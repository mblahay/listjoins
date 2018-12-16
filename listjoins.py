# -*- coding: utf-8 -*-
"""
Created on Tue Dec  4 11:55:33 2018

@author: mb18433
"""

from functools import reduce
import collections

# Convenience methods
def nvl(arg,alt_val=None):
    'Similar concept to NVL function available in databases'
    if arg == None:
        return alt_val
    return arg
    
def zil(arg,alt_val=None): #zil stands for zero item list
    'Allows for the substitution of value if passed in argument has a length of zero'
    if len(arg) == 0:
        return alt_val
    return arg

def nested_loops_join(o,i, okey=None, ikey=None, left_outer=False):
    'Basic nested loops join which allows for inner and left outer joins, First argument may be an iterator or container, while second MUST be a container,'    
    
    default_key = lambda x:x
    ikey,okey = nvl(ikey,default_key),nvl(okey,default_key)

    for ot in o:
        matched = False
        for it in i:
            if okey(ot) == ikey(it):
                matched = True
                yield (ot,it)
        if left_outer and not matched:
            yield (ot,None)
            
    

def inner_join(a,b,sorted=False):
    '''Performs an inner join on the two lists whose records comprise of a key followed by any number of values'
       Each record must be of a type that maintains order, such as a list or tuple.
       The input lists do not need to be sorted, though if it is known that they are sorted then you can tell
       the function that it does not need to bother with sorting them. Sorting is expensive as it requires each of the
       lists to be duplicated and should be avoided if possible for very large datasets.
       The return value of this function is a new list that contains the join result'''

    # Make sure that the two arguments are interators and have substance
    if not all((type(a)==list,type(b)==list)):
        raise TypeError('parameters to inner join must be of type list')
    if min(len(a),len(b))==0: # If at least one of the lists is empty, there is nothing the join with and therefore nothing to return
        return None
 
    if sorted: # If caller set flag that the data is pre-sorted, then just operate directly from the lists that are passed in
        x=a
        y=b
    else: # creating copies of the lists and sort them 
        x=list(a)
        y=list(b)
        x.sort()
        y.sort()
 
    # Storing the lengths of the lists to avoid repeated calls to len
    xl=len(x) 
    yl=len(y)
    
    r=[] # Initializing the list that will hold the newly created list containing the joined data
    xc=yc=0 # Initialising variables for traversing the lists
    while xc < xl and yc < yl: # Keep looking while the counter variables have list to process
        if x[xc][0] == y[yc][0]:
            c=0
            while yc+c < yl and y[yc][0]==y[yc+c][0]:
                r.append((x[xc][0],)+tuple(x[xc][1:])+tuple(y[yc+c][1:]))
                c+=1
            xc+=1
            if xc < xl and x[xc][0]!=x[xc-1][0]: # If we know the next x key is different, then we can move the y counter ahead as well.
                yc+=c
        else: # IF there is no match, increment the counter for whichever list has the least key
            if x[xc][0] > y[yc][0]:
                yc+=1
            else:
                xc+=1
    
    return r


def left_outer_join(a, b, sorted=False):
    '''Performs a left outer join on the two lists whose records comprise of a key followed by any number of values'
       Each record must be of a type that maintains order, such as a list or tuple.
       The input lists do not need to be sorted, though if it is known that they are sorted then you can tell
       the function that it does not need to bother with sorting them. Sorting is expensive as it requires each of the
       lists to be duplicated and should be avoided if possible for very large datasets.
       The return value of this function is a new list that contains the join result'''

    # Make sure that the two arguments are interators and have substance
    if not all((type(a) == list, type(b) == list)):
        raise TypeError('parameters to left_outer_join must be of type list')
    if len(a) == 0:  # If the length of the first list is zero, then there will be nothing to return
        return None

    if sorted:  # If caller set flag that the data is pre-sorted, then just operate directly from the lists that are passed in
        x = a
        y = b
    else:  # creating copies of the lists and sort them
        x = list(a)
        y = list(b)
        x.sort()
        y.sort()

    # Storing the lengths of the lists to avoid repeated calls to len
    xl = len(x)
    yl = len(y)
    ynofr = len(y[0]) # y number of records

    r = []  # Initializing the list that will hold the newly created list containing the joined data
    xc = yc = 0  # Initialising variables for traversing the lists
    while xc < xl:  # Keep looking while the counter variables have list to process
        if yc < yl and x[xc][0] == y[yc][0]:
            c = 0
            while yc + c < yl and y[yc][0] == y[yc + c][0]:
                r.append((x[xc][0],) + tuple(x[xc][1:]) + tuple(y[yc + c][1:]))
                c += 1
            xc += 1
            if xc < xl and x[xc][0] != x[xc - 1][
                0]:  # If we know the next x key is different, then we can move the y counter ahead as well.
                yc += c
        else:  # IF there is no match, increment the counter for whichever list has the least key
            if yc < yl and x[xc][0] > y[yc][0]:
                yc += 1
            else:
                r.append((x[xc][0],) + tuple(x[xc][1:]) + tuple(['']*(ynofr-1))) # There isn't going to be a match for this key, so do the left join and move on.
                xc += 1

    return r

def s_table(listdata, headers, gutterwidth=2):
    'Constructs a formatted table and returns it as a string'
    gutter=' '*gutterwidth # Create the gutter string
    field_widths = list(map(lambda z: max(map(lambda y: len(y), z)), zip(*listdata + [headers]))) # Pivot the data and then find the max width for the columns, including the header strings
    return '\n'.join([
    gutter.join(map(lambda x: x[0].ljust(x[1]), zip(list(headers), field_widths))),  # Print the headers
    gutter.join(map(lambda x: ''.ljust(x, '-'), field_widths)),  # Print lines under the headers
    '\n'.join(map(lambda x: gutter.join(map(lambda y: y[0].ljust(y[1]), zip(list(x), field_widths))), listdata)) # Now print the data
    ])
    # A note about the method being used here. There could be confusion about the use if zip. What is happening here
    # is that the field is being paired up with the corresponding max width using zip. This allows both values to be passed
    # to the function being mapped so that the correct size padding may be added.

def uniq(x,y):
    'For use with functools reduce in order to produce a uniq set of values from a list'
    if type(x)==list:
        if y not in x:
            x.append(y)
        return x
    else:
        return [x,y]
        
        
