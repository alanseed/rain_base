# Runlength encode an array 
import numpy as np  

def encode(rain_rate, min_rain_threshold):
    # encode the input array val,nval,x[n],x[n+1],...,val,nval,x[n],x[n+1],, 
    # val in [0, missing_value] - missing value is assumed to be < 0
    # nval is the length of the set of encoded values 
    # x[n] is the start of a sequence of valid data 
    # Note - the data are scaled (x/0.05) integer values so the minimum value for min_rain_threshold is 1
    # an appropriate value would be 5 or 10  
    # TO DO need to work out if we get nans in the array 

    in_array = rain_rate.flatten()
    
    # set the low values to zero 
    low_value_flag = np.where(in_array > 0 and in_array < min_rain_threshold)
    in_array[low_value_flag] = 0

    # set up the output array
    out_array = np.array(dtype=np.int16)
    
    #initialise the run length variables 
    run_length = 0
    run_val = 1
    
    #start the loop over the input array 
    for ia in range (len(in_array)):
        val = in_array[ia]

        # found valid data 
        if ( val > 0 ):
            # check if we have ended a run of invalid data 
            if run_length > 0:
                out_array.append(run_val)
                out_array.append(run_length)
                run_length = 0
                run_val = 1
            # write out the valid data
            else: 
                out_array.append(val)

        # found invalid data 
        else: 
            # same type of invalid data so continue with existing run 
            if val == run_val:
                run_length += 1 
            # different type of invalid data so start a new run 
            else:
                # write out the existing run 
                if run_length > 0:
                    out_array.append(run_val)
                    out_array.append(run_length)
                # start a new run 
                run_length = 1
                run_val = val
    
    return out_array 

# function to decode the array, returns a 1-D array 
def decode(in_array, out_shape):
    # set up the output array
    
    out_array = np.array(dtype=np.int16)
    
    # loop over the input array 
    ia = 0 
    while ia < len(in_array):
        val = in_array[ia]

        if val <= 0:
            for ib in range(in_array[ia+1]):
                out_array.append(val)
            ia += 2
        else:
            out_array.append(val)
            ia += 1





    return out_array 


