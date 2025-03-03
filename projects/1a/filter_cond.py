#
#
def filter_cond(line_dict):
    """Filter function
    Takes a dict with field names as argument
    Returns True if conditions are satisfied
    """
    try:
    	cond_match = (
       		int(line_dict["if1"]) > 20 and int(line_dict["if1"]) < 40
    	) 
    	return True if cond_match else False
    except:
        return False

