import hashlib


def generate_id(input_string):  # Generate id from input string.
    return hashlib.md5(input_string.encode()).hexdigest()
