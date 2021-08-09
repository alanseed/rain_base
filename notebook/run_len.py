# Runlength encode an array 
import numpy as np  
from scipy import nan 

def encode(rain_rate):
    # encode the input array val,nval,x[n],x[n+1],...,val,nval,x[n],x[n+1],, 
    # val in [0, missing_value] - missing value is assumed to be < 0
    # nval is the length of the set of encoded values 
    # x[n] is the start of a sequence of valid data 
    # Note - the data are scaled (x/0.05) integer values so the minimum value for min_rain_threshold is 1
    # an appropriate value would be 5 or 10  
    
    in_shape = rain_rate.shape
    in_array = rain_rate.flatten()

    out_size = int(1.5*in_shape[0]*in_shape[1]) 
    out_array = np.zeros(out_size,dtype=np.int16)
    out_offset = 0 
    
    #initialise the run length variables 
    run_length = 0
    run_val = 1
    
    #start the loop over the input array 
    for ia in range (len(in_array)):
        val = in_array[ia]
        
        # make sure that we fit inside the allocated memory 
        if out_offset > out_size - 3:
            raise ValueError(f"Error encode: Output encoded array exceeds the allocated size of {out_size} integers")

        # found valid data 
        if ( val > 0 ):
            # check if we have ended a run of invalid data 
            if run_length > 0:
                out_array[out_offset] = np.int16(run_val)
                out_array[out_offset+1] = np.int16(run_length)
                out_array[out_offset+2] = val
                out_offset += 3
                run_length = 0
                run_val = 1
                
            # write out the valid data
            else: 
                out_array[out_offset] = np.int16(val)
                out_offset += 1

        # found invalid data 
        else: 
            # same type of invalid data so continue with existing run 
            if val == run_val:
                run_length += 1 
            # different type of invalid data so start a new run 
            else:
                # write out the existing run 
                if run_length > 0:
                    out_array[out_offset] = np.int16(run_val)
                    out_array[out_offset+1] = np.int16(run_length)
                    out_offset += 2
                # start a new run 
                run_length = 1
                run_val = val
    
    # take care of the last run in the image 
    if ( run_length > 0):
        out_array[out_offset] = np.int16(run_val)
        out_array[out_offset+1] = np.int16(run_length)
        out_offset += 2
             
    out_array = np.resize(out_array,out_offset)
    return out_array 

# function to decode the array 
def decode(in_array, out_shape):
    # set up the output array
    out_size = out_shape[0]*out_shape[1]
    out_array = np.zeros(out_size,dtype=np.int64)

    # loop over the input array 
    in_offset = 0
    out_offset = 0 
    while in_offset < len(in_array):
        val = in_array[in_offset]

        if val <= 0:
            run_length = in_array[in_offset+1]
            for ib in range(run_length):
                out_array[out_offset+ib] = val
            in_offset += 2
            out_offset += run_length

        else:
            out_array[out_offset] = val
            in_offset += 1
            out_offset += 1

    out_array = np.reshape(out_array, out_shape)
    return out_array 

# # single rain domain with missing and zero data 
# def test():
#     in_shape = (10,10)
#     in_array = np.zeros(in_shape,dtype=np.float64)
#     for irow in range(in_shape[0]):
#         for icol in range(5,in_shape[1]):
#             in_array[irow][icol] = icol 

#     for irow in range(in_shape[0]):
#         for icol in range(2,4):
#             in_array[irow][icol] = nan

#     return in_array

# in_array = test()
# print("input test array")
# print(in_array)

# out_array = encode(in_array) 
# print("output encoded array")
# print(out_array)

# test_array = decode(out_array, in_array.shape)
# print ("test array - encoded then decoded test array")
# print(test_array-in_array)