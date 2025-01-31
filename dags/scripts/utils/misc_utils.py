def find_key_containing_string(search_string, my_dict):
    for key in my_dict.keys():
        if search_string in key:
            return key
    return None